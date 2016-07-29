// The MIT License (MIT)
//
// Copyright (c) 2016 Tim Jones
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package io.github.jonestimd.neo4j.client.transaction;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.github.jonestimd.neo4j.client.http.HttpDriver;
import io.github.jonestimd.neo4j.client.http.HttpResponse;
import io.github.jonestimd.neo4j.client.transaction.request.Statement;
import io.github.jonestimd.neo4j.client.transaction.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to execute Cypher queries in a transaction.  No HTTP requests are made until the
 * {@link #execute(Statement...) execute()} or {@link #commit(Statement...) commit()} method is called with at least one
 * {@link Statement}.  Once {@link #execute(Statement...) execute()} is called with a {@link Statement}, a timer task is
 * scheduled to ping the transaction URL periodically to keep the transaction alive until it is complete.  This
 * timer task is disabled if the transaction is created with a {@code null} {@link Timer}.
 */
public class Transaction {
    public static final JsonFactory DEFAULT_JSON_FACTORY = new JsonFactory();
    public static final String TRANSACTION_COMPLETE_ERROR = "Transaction already complete";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final JsonFactory jsonFactory;
    private final HttpDriver httpDriver;
    private final String baseUrl;
    private final Timer timer;
    private final long keepAliveMs;
    private volatile String location;
    private volatile boolean complete = false;
    private volatile long lastRequestTime = -1L;

    /**
     * Create a new transaction with the keep alive task disabled.
     * @param httpDriver the HTTP driver to use for requests
     * @param baseUrl the base URL for the Neo4j transaction REST API
     */
    public Transaction(HttpDriver httpDriver, String baseUrl) {
        this(httpDriver, baseUrl, null, 0L);
    }

    /**
     * Create a new transaction using the default {@link JsonFactory}.
     * @param httpDriver the HTTP driver to use for requests
     * @param baseUrl the base URL for the Neo4j transaction REST API
     * @param timer the timer to use for scheduling the keep alive task
     * @param keepAliveMs the period of the keep alive requests in milliseconds
     */
    public Transaction(HttpDriver httpDriver, String baseUrl, Timer timer, long keepAliveMs) {
        this(httpDriver, baseUrl, null, timer, keepAliveMs);
    }

    /**
     * Create a new transaction.
     * @param httpDriver the HTTP driver to use for requests
     * @param baseUrl the base URL for the Neo4j transaction REST API
     * @param jsonFactory factory for creating generators and parsers
     * @param timer the timer to use for scheduling the keep alive task
     * @param keepAliveMs the period of the keep alive requests in milliseconds
     */
    public Transaction(HttpDriver httpDriver, String baseUrl, JsonFactory jsonFactory, Timer timer, long keepAliveMs) {
        this.jsonFactory = jsonFactory != null ? jsonFactory : DEFAULT_JSON_FACTORY;
        this.httpDriver = httpDriver;
        this.baseUrl = baseUrl;
        this.timer = timer;
        this.keepAliveMs = keepAliveMs;
    }

    /**
     * @return true if this transaction has been committed or rolled back.
     */
    public boolean isComplete() {
        return complete;
    }

    /**
     * Execute a group of Cypher queries within this transaction.
     * @param statements the Cypher queries
     * @return the result of the Cypher queries
     * @throws IOException
     * @throws IllegalStateException if this transaction is complete
     */
    public Response execute(Statement... statements) throws IOException {
        if (complete) throw new IllegalStateException(TRANSACTION_COMPLETE_ERROR);
        if (statements.length > 0) return postRequest(getUri(), statements);
        return Response.EMPTY;
    }

    /**
     * Execute a group of Cypher queries within this transaction and commit the transaction.
     * @param statements the Cypher queries
     * @return the result of the Cypher queries
     * @throws IOException
     * @throws IllegalStateException if this transaction is complete
     */
    public Response commit(Statement... statements) throws IOException {
        if (complete) throw new IllegalStateException(TRANSACTION_COMPLETE_ERROR);
        if (statements.length == 0 && location == null) return Response.EMPTY;
        Response response = postRequest(getUri() + "/commit", statements);
        complete = true;
        return response;
    }

    protected String getUri() {
        return location == null ? baseUrl : location;
    }

    protected Response postRequest(String uri, Statement... statements) throws IOException {
        lastRequestTime = System.currentTimeMillis();
        try (HttpResponse httpResponse = httpDriver.post(uri, toJson(statements))) {
            updateLocation(httpResponse.getHeader("Location"));
            return new Response(jsonFactory.createParser(httpResponse.getEntityContent()));
        }
    }

    private void updateLocation(String location) {
        if (location != null && ! location.equals(this.location)) {
            this.location = location;
            if (timer != null) {
                timer.schedule(new PingTask(), keepAliveMs);
            }
        }
    }

    private String toJson(Statement... statements) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (JsonGenerator generator = jsonFactory.createGenerator(stream)) {
            writeStatements(generator, statements);
        }
        return stream.toString("UTF-8");
    }

    private void writeStatements(JsonGenerator generator, Statement... statements) throws IOException {
        generator.writeStartObject();
        generator.writeArrayFieldStart("statements");
        for (Statement statement : statements) {
            statement.toJson(generator);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    /**
     * Rollback this transaction.
     * @return the result of rolling back the transaction
     * @throws IOException
     * @throws IllegalStateException if this transaction is complete
     */
    public Response rollback() throws IOException {
        if (complete) throw new IllegalStateException(TRANSACTION_COMPLETE_ERROR);
        if (location != null) {
            try (HttpResponse httpResponse = httpDriver.delete(location)) {
                this.complete = true;
                return new Response(jsonFactory.createParser(httpResponse.getEntityContent()));
            }
        }
        return Response.EMPTY;
    }

    private class PingTask extends TimerTask {
        @Override
        public void run() {
            if (!complete) {
                long now = System.currentTimeMillis();
                if (now - lastRequestTime >= keepAliveMs) {
                    try {
                        lastRequestTime = System.currentTimeMillis();
                        postRequest(location).next();
                    } catch (Throwable ex) {
                        logger.warn("error pinging transaction URL " + location, ex);
                    }
                }
                timer.schedule(new PingTask(), lastRequestTime + keepAliveMs - now);
            }
        }
    }
}