// The MIT License (MIT)
//
// Copyright (c) 2016 Tim Jones
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * This class represents metadata for a column value in a row of a query result.  For a node or relationship column, a list
 * of one {@code ColumnMeta} is returned.  For a path column, the list contains one {@code ColumnMeta} for each path element.
 */
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

    /**
     * Get the graph ID of the node or relationship.
     */
    public Long getId() {
        return id;
    }

    /**
     * Get the graph element type.
     */
    public MetaType getType() {
        return type;
    }

    public boolean isDeleted() {
        return deleted;
    }

    /**
     * Read the next column metadata from a JSON stream.
     * @param parser the JSON parser for the stream
     * @return the metadata for the column
     * @throws IOException
     */
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
