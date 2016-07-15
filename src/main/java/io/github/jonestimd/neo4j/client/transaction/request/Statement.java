package io.github.jonestimd.neo4j.client.transaction.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import io.github.jonestimd.neo4j.client.ToJson;

public class Statement implements ToJson {
    private final boolean includeStats;
    private final List<ResultType> resultTypes = new ArrayList<>();
    private final String query;
    private final Map<String, ?> parameters;

    /**
     * Default statistics to {@code false} and results to both {@link ResultType#Row} and {@link ResultType#Graph}.
     */
    public Statement(String query, Map<String, ?> parameters) {
        this(query, parameters, false, ResultType.Row, ResultType.Graph);
    }

    public Statement(String query, Map<String, ?> parameters, boolean includeStats, ResultType... resultTypes) {
        this.query = query;
        this.includeStats = includeStats;
        Collections.addAll(this.resultTypes, resultTypes);
        this.parameters = parameters;
    }

    public String getQuery() {
        return query;
    }

    public Map<String, ?> getParameters() {
        return parameters == null ? null : Collections.unmodifiableMap(parameters);
    }

    public boolean isIncludeStats() {
        return includeStats;
    }

    public List<ResultType> getResultTypes() {
        return Collections.unmodifiableList(resultTypes);
    }

    public void toJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("statement", query);
        if (parameters != null && ! parameters.isEmpty()) {
            generator.writeFieldName("parameters");
            writeObject(generator, parameters);
        }
        if (includeStats) generator.writeBooleanField("includeStats", true);
        if (! resultTypes.isEmpty()) {
            generator.writeArrayFieldStart("resultDataContents");
            for (ResultType resultType : resultTypes) {
                resultType.toJson(generator);
            }
            generator.writeEndArray();
        }
        generator.writeEndObject();
    }

    @SuppressWarnings("unchecked")
    private void writeField(JsonGenerator generator, String fieldName, Object value) throws IOException {
        generator.writeFieldName(fieldName);
        writeValue(generator, value);
    }

    private void writeObject(JsonGenerator generator, Map<String, ?> properties) throws IOException {
        generator.writeStartObject();
        for (Entry<String, ?> entry : properties.entrySet()) {
            writeField(generator, entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();
    }

    private void writeArray(JsonGenerator generator, Collection<?> value) throws IOException {
        generator.writeStartArray();
        for (Object item : value) {
            writeValue(generator, item);
        }
        generator.writeEndArray();
    }

    @SuppressWarnings("unchecked")
    private void writeValue(JsonGenerator generator, Object value) throws IOException {
        if (value instanceof ToJson) {
            ((ToJson) value).toJson(generator);
        }
        else if (value instanceof Map) {
            writeObject(generator, (Map) value);
        }
        else if (value instanceof Collection) {
            writeArray(generator, (Collection) value);
        }
        else generator.writeObject(value);
    }
}
