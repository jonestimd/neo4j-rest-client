package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.ByteArrayInputStream;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.github.jonestimd.neo4j.client.transaction.response.ColumnMeta.MetaType;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;

public class ColumnMetaTest {
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    public void readNullMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[null]".getBytes()));
        parser.nextToken();

        assertThat(ColumnMeta.read(parser)).containsExactly((Object) null);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
    }

    @Test
    public void readNodeColumnMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[{\"id\":1,\"type\":\"node\",\"deleted\":false}]".getBytes()));
        parser.nextToken();

        List<ColumnMeta> metas = ColumnMeta.read(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(metas).hasSize(1);
        verifyMeta(metas.get(0), 1L, MetaType.NODE);
    }

    @Test
    public void readRelationshipColumnMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[{\"id\":1,\"type\":\"relationship\",\"deleted\":false}]".getBytes()));
        parser.nextToken();

        List<ColumnMeta> metas = ColumnMeta.read(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(metas).hasSize(1);
        verifyMeta(metas.get(0), 1L, MetaType.RELATIONSHIP);
    }

    @Test
    public void readPathColumnMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(("[[" +
                "{\"id\":1,\"type\":\"node\",\"deleted\":false}," +
                "{\"id\":2,\"type\":\"relationship\",\"deleted\":false}," +
                "{\"id\":3,\"type\":\"node\",\"deleted\":false}]]").getBytes()));
        parser.nextToken();

        List<ColumnMeta> metas = ColumnMeta.read(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(metas).hasSize(3);
        verifyMeta(metas.get(0), 1L, MetaType.NODE);
        verifyMeta(metas.get(1), 2L, MetaType.RELATIONSHIP);
        verifyMeta(metas.get(2), 3L, MetaType.NODE);
    }

    private void verifyMeta(ColumnMeta meta, long id, MetaType type) {
        assertThat(meta.getId()).isEqualTo(id);
        assertThat(meta.getType()).isEqualTo(type);
        assertThat(meta.isDeleted()).isFalse();
    }
}