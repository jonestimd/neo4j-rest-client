package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.github.jonestimd.neo4j.client.transaction.response.ColumnMeta.MetaType;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;

public class ResultRowTest {
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    public void readResultRow() throws Exception {
        String json = "{\"row\":[{\"p1\":100,\"p2\":\"value1\"},\"value2\"]," +
                "\"meta\":[{\"id\":1,\"type\":\"node\",\"deleted\":false},null]," +
                "\"graph\":{" +
                "\"nodes\":[" +
                "{\"id\":\"1\",\"labels\":[\"Label\"],\"properties\":{\"p1\":100,\"p2\":\"value1\"}}," +
                "{\"id\":\"3\",\"labels\":[\"Label\"],\"properties\":{\"p1\":101,\"p2\":\"value9\"}}]," +
                "\"relationships\":[{\"id\":2,\"type\":\"R1\",\"startNode\":1,\"endNode\":2,\"properties\":{}}]}}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        parser.nextToken();

        ResultRow row = ResultRow.read(Arrays.asList("c1", "c2"), parser);

        assertThat(parser.getCurrentToken()).isEqualTo(JsonToken.END_OBJECT);
        assertThat(parser.nextToken()).isNull();
        assertThat(row.getColumn("c1").getProperties().keySet()).containsOnly("p1", "p2");
        assertThat(row.getColumn("c2").getString()).isEqualTo(Optional.of("value2"));
        assertThat(row.getMeta("c1")).hasSize(1);
        assertThat(row.getMeta("c1").get(0).getId()).isEqualTo(1L);
        assertThat(row.getMeta("c1").get(0).getType()).isEqualTo(MetaType.NODE);
        assertThat(row.getMeta("c1").get(0).isDeleted()).isFalse();
        assertThat(row.getMeta("c2")).containsExactly((Object) null);
        assertThat(row.getNodes()).hasSize(2);
        assertThat(row.getRelationships()).hasSize(1);
    }
}