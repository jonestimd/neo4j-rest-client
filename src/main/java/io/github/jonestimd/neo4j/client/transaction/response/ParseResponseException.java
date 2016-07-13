package io.github.jonestimd.neo4j.client.transaction.response;

import com.fasterxml.jackson.core.JsonLocation;

public class ParseResponseException extends RuntimeException {
    private final JsonLocation location;

    public ParseResponseException(JsonLocation location) {
        this.location = location;
    }

    public JsonLocation getLocation() {
        return location;
    }
}
