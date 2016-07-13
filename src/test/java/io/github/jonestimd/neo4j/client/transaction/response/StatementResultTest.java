package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.ByteArrayInputStream;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.github.jonestimd.neo4j.client.transaction.response.ColumnMeta.MetaType;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;

public class StatementResultTest {
    public static final String ROW = "{\"row\":[{\"p1\":100,\"p2\":\"value1\"},\"value2\"]," +
            "\"meta\":[{\"id\":1,\"type\":\"node\",\"deleted\":false},null]," +
            "\"graph\":{" +
            "\"nodes\":[" +
            "{\"id\":\"1\",\"labels\":[\"Label\"],\"properties\":{\"p1\":100,\"p2\":\"value1\"}}," +
            "{\"id\":\"3\",\"labels\":[\"Label\"],\"properties\":{\"p1\":101,\"p2\":\"value9\"}}]," +
            "\"relationships\":[{\"id\":2,\"type\":\"R1\",\"startNode\":1,\"endNode\":2,\"properties\":{}}]}}";
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    public void emptyResult() throws Exception {
        String json = "{\"columns\":[],\"data\":[]}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        assert parser.nextToken() == JsonToken.START_OBJECT;

        StatementResult result = new StatementResult(parser);

        assertThat(result.next()).isFalse();
        assertThat(result.next()).isFalse();
        assertThat(parser.nextToken()).isNull();
    }

    @Test
    public void readResultRows() throws Exception {
        String json = "{\"columns\":[\"c1\",\"c2\"],\"data\":[" + ROW + "]}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        assert parser.nextToken() == JsonToken.START_OBJECT;

        StatementResult result = new StatementResult(parser);

        assertThat(result.next()).isTrue();
        assertThat(result.getColumn("c1").getProperties().keySet()).containsOnly("p1", "p2");
        assertThat(result.getColumn("c2").getString()).isEqualTo(Optional.of("value2"));
        assertThat(result.getMeta("c1")).hasSize(1);
        assertThat(result.getMeta("c1").get(0).getId()).isEqualTo(1L);
        assertThat(result.getMeta("c1").get(0).getType()).isEqualTo(MetaType.NODE);
        assertThat(result.getMeta("c1").get(0).isDeleted()).isFalse();
        assertThat(result.getMeta("c2")).containsExactly((Object) null);
        assertThat(result.getNodes()).hasSize(2);
        assertThat(result.getNodesById()).hasSize(2);
        assertThat(result.getRelationships()).hasSize(1);
        assertThat(result.next()).isFalse();
        assertThat(parser.nextToken()).isNull();
    }

    @Test
    public void nextSkipsUnknownProperties() throws Exception {
        String json = "{\"columns\":[\"c1\",\"c2\"],\"unknown\":{},\"data\":[" + ROW + "],\"ignored\":[],\"enhancement\":true}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        assert parser.nextToken() == JsonToken.START_OBJECT;
        StatementResult result = new StatementResult(parser);

        assertThat(result.next()).isTrue();
        assertThat(result.next()).isFalse();

        assertThat(parser.nextToken()).isNull();
    }
}