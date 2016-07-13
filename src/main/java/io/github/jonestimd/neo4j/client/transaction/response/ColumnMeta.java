package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ColumnMeta {
    public enum MetaType { NODE, RELATIONSHIP }

    private final Long id;
    private final MetaType type;
    private final boolean deleted;

    public ColumnMeta() {
        this(null, null, false);
    }

    private ColumnMeta(Long id, MetaType type, boolean deleted) {
        this.id = id;
        this.type = type;
        this.deleted = deleted;
    }

    public Long getId() {
        return id;
    }

    public MetaType getType() {
        return type;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public static List<ColumnMeta> read(JsonParser parser) throws IOException {
        switch (parser.nextToken()) {
            case START_OBJECT: return Collections.singletonList(parseMeta(parser));
            case START_ARRAY: return parseArray(parser);
            case VALUE_NULL: return Collections.singletonList(null);
            default: throw new ParseResponseException(parser.getCurrentLocation());
        }
    }

    private static List<ColumnMeta> parseArray(JsonParser parser) throws IOException {
        List<ColumnMeta> metas = new ArrayList<>();
        JsonToken event;
        while ((event = parser.nextToken()) != JsonToken.END_ARRAY) {
            if (event == JsonToken.START_OBJECT) metas.add(parseMeta(parser));
            else if (event == JsonToken.VALUE_NULL) metas.add(null);
            else throw new ParseResponseException(parser.getCurrentLocation());
        }
        return metas;
    }

    private static ColumnMeta parseMeta(JsonParser parser) throws IOException {
        assert parser.getCurrentToken() == JsonToken.START_OBJECT;
        Long id = null;
        MetaType type = null;
        boolean deleted = false;
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            if (parser.getCurrentName().equals("id")) {
                assert parser.nextToken() == JsonToken.VALUE_NUMBER_INT;
                id = parser.getLongValue();
            }
            else if (parser.getCurrentName().equals("type")) {
                assert parser.nextToken() == JsonToken.VALUE_STRING;
                type = MetaType.valueOf(parser.getText().toUpperCase());
            }
            else if (parser.getCurrentName().equals("deleted")) {
                deleted = parser.nextToken() == JsonToken.VALUE_TRUE;
            }
            else {
                parser.nextToken();
            }
        }
        return new ColumnMeta(id, type, deleted);
    }
}
