package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParser;

public class ResultColumn {
    private final Object value;

    public ResultColumn(JsonParser parser) throws IOException {
        this.value = JsonReader.readNext(parser);
    }

    public Optional<String> getString() {
        return Optional.of((String) value);
    }

    public Optional<Number> getNumber() {
        return Optional.of((Number) value);
    }

    @SuppressWarnings("unchecked")
    public Map<String, ?> getProperties() {
        return (Map<String, ?>) value;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList() {
        return value == null ? Collections.emptyList() : (List<T>) value;
    }
}
