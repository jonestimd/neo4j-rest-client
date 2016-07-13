package io.github.jonestimd.neo4j.client.transaction.request;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import io.github.jonestimd.neo4j.client.ToJson;

public enum ResultType implements ToJson {
    Row, Graph;

    public void toJson(JsonGenerator generator) throws IOException {
        generator.writeString(name().toLowerCase());
    }
}
