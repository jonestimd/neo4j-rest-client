package io.github.jonestimd.neo4j.client;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;

public interface ToJson {
    void toJson(JsonGenerator generator) throws IOException;
}
