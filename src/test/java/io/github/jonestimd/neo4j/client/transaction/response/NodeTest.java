package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.ByteArrayInputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;

public class NodeTest {
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    public void readWithoutProperties() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("{\"id\":\"1\",\"labels\":[\"l1\"]}".getBytes()));
        parser.nextToken();

        Node node = Node.read(parser);

        assertThat(parser.getCurrentToken()).isEqualTo(JsonToken.END_OBJECT);
        assertThat(parser.nextToken()).isNull();
        assertThat(node.getId()).isEqualTo(1L);
        assertThat(node.getLabels()).containsOnly("l1");
        assertThat(node.getProperties()).isEmpty();
    }

    @Test
    public void readWithProperties() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("{\"id\":\"1\",\"labels\":[\"l1\"],\"properties\":{\"p1\":99}}".getBytes()));
        parser.nextToken();

        Node node = Node.read(parser);

        assertThat(parser.getCurrentToken()).isEqualTo(JsonToken.END_OBJECT);
        assertThat(parser.nextToken()).isNull();
        assertThat(node.getId()).isEqualTo(1L);
        assertThat(node.getLabels()).containsOnly("l1");
        assertThat(node.getProperties().keySet()).containsOnly("p1");
        assertThat(node.getProperties().get("p1")).isEqualTo(99L);
    }
}