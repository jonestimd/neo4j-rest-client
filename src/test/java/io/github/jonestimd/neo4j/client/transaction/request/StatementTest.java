package io.github.jonestimd.neo4j.client.transaction.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.github.jonestimd.neo4j.client.ToJson;
import org.junit.Test;

import static java.util.Collections.*;
import static org.fest.assertions.Assertions.*;

public class StatementTest {
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Test
    public void includeStatsDefaultsToFalse() throws Exception {
        assertThat(new Statement(null, null).isIncludeStats()).isFalse();
    }

    @Test
    public void resultTypesDefaultsToRowAndGraph() throws Exception {
        assertThat(new Statement(null, null).getResultTypes()).containsOnly(ResultType.Graph, ResultType.Row);
    }

    @Test
    public void toJsonHandlesNulParameters() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = JSON_FACTORY.createGenerator(stream);
        Statement statement = new Statement("cypher query", null, true, ResultType.Graph);

        statement.toJson(generator);

        generator.close();
        assertThat(stream.toString()).isEqualTo(
                "{\"statement\":\"cypher query\"," +
                "\"includeStats\":true," +
                "\"resultDataContents\":[\"graph\"]}");
        assertThat(statement.getQuery()).isEqualTo("cypher query");
        assertThat(statement.getParameters()).isNull();
    }

    @Test
    public void toJsonHandlesEmptyParameters() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = JSON_FACTORY.createGenerator(stream);
        Statement statement = new Statement("cypher query", emptyMap(), true);

        statement.toJson(generator);

        generator.close();
        assertThat(stream.toString()).isEqualTo("{\"statement\":\"cypher query\",\"includeStats\":true}");
        assertThat(statement.getQuery()).isEqualTo("cypher query");
        assertThat(statement.getParameters()).isEmpty();
    }

    @Test
    public void toJsonHandlesSimpleParameterValue() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = JSON_FACTORY.createGenerator(stream);
        Map<String, ?> parameters = singletonMap("param", "value");
        Statement statement = new Statement("cypher query", parameters);

        statement.toJson(generator);

        generator.close();
        assertThat(stream.toString()).isEqualTo(
                "{\"statement\":\"cypher query\"," +
                "\"parameters\":{\"param\":\"value\"}," +
                "\"resultDataContents\":[\"row\",\"graph\"]}");
        assertThat(statement.getQuery()).isEqualTo("cypher query");
        assertThat(statement.getParameters()).isEqualTo(parameters);
    }

    @Test
    public void toJsonHandlesListParameterValue() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = JSON_FACTORY.createGenerator(stream);
        Map<String, ?> parameters = singletonMap("param", Arrays.asList(1, 2, 3));
        Statement statement = new Statement("cypher query", parameters);

        statement.toJson(generator);

        generator.close();
        assertThat(stream.toString()).isEqualTo(
                "{\"statement\":\"cypher query\"," +
                "\"parameters\":{\"param\":[1,2,3]}," +
                "\"resultDataContents\":[\"row\",\"graph\"]}");
    }

    @Test
    public void toJsonHandlesMapParameterValue() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = JSON_FACTORY.createGenerator(stream);
        Map<String, ?> parameters = singletonMap("param", singletonMap("nested", "value"));
        Statement statement = new Statement("cypher query", parameters);

        statement.toJson(generator);

        generator.close();
        assertThat(stream.toString()).isEqualTo(
                "{\"statement\":\"cypher query\"," +
                "\"parameters\":{\"param\":{\"nested\":\"value\"}}," +
                "\"resultDataContents\":[\"row\",\"graph\"]}");
    }

    @Test
    public void toJsonHandlesCustomParameterValue() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator generator = JSON_FACTORY.createGenerator(stream);
        Map<String, ?> parameters = singletonMap("param", new CustomValue());
        Statement statement = new Statement("cypher query", parameters);

        statement.toJson(generator);

        generator.close();
        assertThat(stream.toString()).isEqualTo(
                "{\"statement\":\"cypher query\"," +
                "\"parameters\":{\"param\":\"generated JSON here\"}," +
                "\"resultDataContents\":[\"row\",\"graph\"]}");
    }

    private static class CustomValue implements ToJson {
        @Override
        public void toJson(JsonGenerator generator) throws IOException {
            generator.writeString("generated JSON here");
        }
    }
}