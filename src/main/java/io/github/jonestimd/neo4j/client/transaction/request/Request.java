package io.github.jonestimd.neo4j.client.transaction.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.github.jonestimd.neo4j.client.ToJson;

public class Request implements ToJson {
    private final List<Statement> statements = new ArrayList<>();

    public Request(Statement... statements) {
        this(Arrays.asList(statements));
    }

    public Request(List<Statement> statements) {
        this.statements.addAll(statements);
    }

    public List<Statement> getStatements() {
        return Collections.unmodifiableList(statements);
    }

    public String toJson() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = new JsonFactory().createGenerator(stream);
        toJson(generator);
        generator.close();
        return stream.toString("UTF-8");
    }

    @Override
    public void toJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        writeStatements(generator);
        generator.writeEndObject();
    }

    private void writeStatements(JsonGenerator generator) throws IOException {
        generator.writeArrayFieldStart("statements");
        for (Statement statement : statements) {
            statement.toJson(generator);
        }
        generator.writeEndArray();
    }
}
