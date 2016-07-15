package io.github.jonestimd.neo4j.client.transaction;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import io.github.jonestimd.neo4j.client.http.HttpDriver;
import io.github.jonestimd.neo4j.client.http.HttpResponse;
import io.github.jonestimd.neo4j.client.transaction.request.Request;
import io.github.jonestimd.neo4j.client.transaction.request.Statement;
import io.github.jonestimd.neo4j.client.transaction.response.Response;

public class Transaction {
    private static final JsonFactory DEFAULT_JSON_FACTORY = new JsonFactory();

    private final JsonFactory jsonFactory;
    private final HttpDriver httpDriver;
    private final String baseUrl;
    private String location;
    private boolean complete = false;

    public Transaction(HttpDriver httpDriver, String baseUrl) {
        this(httpDriver, baseUrl, DEFAULT_JSON_FACTORY);
    }

    public Transaction(HttpDriver httpDriver, String baseUrl, JsonFactory jsonFactory) {
        this.jsonFactory = jsonFactory;
        this.httpDriver = httpDriver;
        this.baseUrl = baseUrl;
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
        try (HttpResponse httpResponse = httpDriver.post(uri, request.toJson())) {
            updateLocation(httpResponse.getHeader("Location"));
            return new Response(jsonFactory.createParser(httpResponse.getEntityContent()));
        }
    }

    private void updateLocation(String location) {
        if (location != null) {
            // TODO start keep alive timer
            this.location = location;
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
}