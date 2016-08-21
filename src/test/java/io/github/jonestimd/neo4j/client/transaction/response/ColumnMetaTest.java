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
    public void defaultConstructor() throws Exception {
        ColumnMeta meta = new ColumnMeta();

        assertThat(meta.getId()).isNull();
        assertThat(meta.getType()).isNull();
        assertThat(meta.isDeleted()).isFalse();
    }

    @Test
    public void readNullMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[null]".getBytes()));
        parser.nextToken();

        assertThat(ColumnMeta.read(parser)).containsExactly((Object) null);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
    }

    @Test
    public void readMultiColumnNullMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[[null,null]]".getBytes()));
        parser.nextToken();

        assertThat(ColumnMeta.read(parser)).containsExactly(null, null);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
    }

    @Test(expected = ParseResponseException.class)
    public void invalidColumnMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[\"x\"]".getBytes()));
        parser.nextToken();

        ColumnMeta.read(parser);
    }

    @Test(expected = ParseResponseException.class)
    public void invalidMultiColumnMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[[null,\"x\"]]".getBytes()));
        parser.nextToken();

        ColumnMeta.read(parser);
    }

    @Test
    public void readNodeColumnMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(("[[{\"id\":1,\"type\":\"node\",\"deleted\":false}," +
                "{\"id\":2,\"type\":\"node\",\"deleted\":true,\"ignored\":\"???\"}]]").getBytes()));
        parser.nextToken();

        List<ColumnMeta> metas = ColumnMeta.read(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(metas).hasSize(2);
        verifyMeta(metas.get(0), 1L, MetaType.NODE, false);
        verifyMeta(metas.get(1), 2L, MetaType.NODE, true);
    }

    @Test
    public void readRelationshipColumnMeta() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[{\"id\":1,\"type\":\"relationship\",\"deleted\":false}]".getBytes()));
        parser.nextToken();

        List<ColumnMeta> metas = ColumnMeta.read(parser);

        assertThat(parser.nextToken()).isEqualTo(JsonToken.END_ARRAY);
        assertThat(metas).hasSize(1);
        verifyMeta(metas.get(0), 1L, MetaType.RELATIONSHIP, false);
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
        verifyMeta(metas.get(0), 1L, MetaType.NODE, false);
        verifyMeta(metas.get(1), 2L, MetaType.RELATIONSHIP, false);
        verifyMeta(metas.get(2), 3L, MetaType.NODE, false);
    }

    private void verifyMeta(ColumnMeta meta, long id, MetaType type, boolean deleted) {
        assertThat(meta.getId()).isEqualTo(id);
        assertThat(meta.getType()).isEqualTo(type);
        assertThat(meta.isDeleted()).isEqualTo(deleted);
    }
}