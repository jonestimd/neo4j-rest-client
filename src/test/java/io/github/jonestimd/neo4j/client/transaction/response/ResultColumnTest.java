package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;

public class ResultColumnTest {
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    public void createStringResult() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[\"value\"]".getBytes()));
        parser.nextToken();

        ResultColumn column = new ResultColumn(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(column.getString().get()).isEqualTo("value");
    }

    @Test
    public void createLongResult() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[123]".getBytes()));
        parser.nextToken();

        ResultColumn column = new ResultColumn(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(column.getNumber().get().longValue()).isEqualTo(123L);
    }

    @Test
    public void createFloatResult() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[123.456]".getBytes()));
        parser.nextToken();

        ResultColumn column = new ResultColumn(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(column.getNumber().get()).isInstanceOf(BigDecimal.class);
        assertThat(column.getNumber().get().toString()).isEqualTo("123.456");
    }

    @Test
    public void createBooleanResult() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[true]".getBytes()));
        parser.nextToken();

        ResultColumn column = new ResultColumn(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(column.getBoolean().get()).isEqualTo(true);
    }

    @Test
    public void createListResult() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[[\"value1\",\"value2\"]]".getBytes()));
        parser.nextToken();

        ResultColumn column = new ResultColumn(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(column.getList()).containsExactly("value1", "value2");
    }

    @Test
    public void nullListResult() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[null]".getBytes()));
        parser.nextToken();

        ResultColumn column = new ResultColumn(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(column.getList()).isEmpty();
    }

    @Test
    public void createObjectResult() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[{\"p1\":\"value1\",\"p2\":\"value2\"}]".getBytes()));
        parser.nextToken();

        ResultColumn column = new ResultColumn(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(column.getProperties()).hasSize(2);
        assertThat(column.getProperties().get("p1")).isEqualTo("value1");
        assertThat(column.getProperties().get("p2")).isEqualTo("value2");
    }
}