package io.github.jonestimd.neo4j.client.transaction;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import com.fasterxml.jackson.core.JsonFactory;
import io.github.jonestimd.neo4j.client.http.HttpDriver;
import io.github.jonestimd.neo4j.client.http.HttpResponse;
import io.github.jonestimd.neo4j.client.transaction.request.Request;
import io.github.jonestimd.neo4j.client.transaction.request.Statement;
import io.github.jonestimd.neo4j.client.transaction.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transaction {
    private static final JsonFactory DEFAULT_JSON_FACTORY = new JsonFactory();

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final JsonFactory jsonFactory;
    private final HttpDriver httpDriver;
    private final String baseUrl;
    private final Timer timer;
    private final long keepAliveMs;
    private volatile String location;
    private volatile boolean complete = false;
    private volatile long lastRequestTime = -1L;

    public Transaction(HttpDriver httpDriver, String baseUrl) {
        this(httpDriver, baseUrl, null, 0L);
    }

    public Transaction(HttpDriver httpDriver, String baseUrl, Timer timer, long keepAliveMs) {
        this(httpDriver, baseUrl, DEFAULT_JSON_FACTORY, timer, keepAliveMs);
    }

    public Transaction(HttpDriver httpDriver, String baseUrl, JsonFactory jsonFactory, Timer timer, long keepAliveMs) {
        this.jsonFactory = jsonFactory;
        this.httpDriver = httpDriver;
        this.baseUrl = baseUrl;
        this.timer = timer;
        this.keepAliveMs = keepAliveMs;
    }

    public boolean isComplete() {
        return complete;
    }

    public Response execute(Statement... statements) throws IOException {
        if (complete) throw new IllegalStateException("Transaction already complete");
        if (statements.length > 0) return postRequest(new Request(statements), getUri());
        return Response.EMPTY;
    }

    public Response commit(Statement... statements) throws IOException {
        if (complete) throw new IllegalStateException("Transaction already complete");
        if (statements.length == 0 && location == null) return Response.EMPTY;
        Response response = postRequest(new Request(statements), getUri() + "/commit");
        complete = true;
        return response;
    }

    protected String getUri() {
        return location == null ? baseUrl : location;
    }

    private Response postRequest(Request request, String uri) throws IOException {
        lastRequestTime = System.currentTimeMillis();
        try (HttpResponse httpResponse = httpDriver.post(uri, request.toJson(jsonFactory))) {
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

    public Response rollback() throws IOException {
        if (complete) throw new IllegalStateException("Transaction already complete");
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
                        postRequest(new Request(), location).next();
                    } catch (Throwable ex) {
                        logger.warn("error pinging transaction URL " + location, ex);
                    }
                }
                timer.schedule(new PingTask(), lastRequestTime + keepAliveMs - now);
            }
        }
    }
}