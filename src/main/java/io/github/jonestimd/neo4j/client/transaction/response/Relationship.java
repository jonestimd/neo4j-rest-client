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
import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * This class represents a graph relationship in a query result.
 */
public class Relationship implements GraphElement {
    private final Long id;
    private final String type;
    private final Long startId;
    private final Long endId;
    private final Map<String, Object> properties;

    public Relationship() {
        this(null, null, null, null, Collections.emptyMap());
    }

    public Relationship(Long id, String type, Long startId, Long endId, Map<String, Object> properties) {
        this.id = id;
        this.type = type;
        this.startId = startId;
        this.endId = endId;
        this.properties = Collections.unmodifiableMap(properties);
    }

    /**
     * Get the graph ID of this relationship.
     */
    @Override
    public Long getId() {
        return id;
    }

    /**
     * Get the type of this relationship.
     */
    public String getType() {
        return type;
    }

    /**
     * Get the graph ID of the starting node.
     */
    public Long getStartId() {
        return startId;
    }

    /**
     * Get the graph ID of the ending node.
     */
    public Long getEndId() {
        return endId;
    }

    /**
     * Get the properties attached to this relationship.
     */
    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    public static Relationship read(JsonParser parser) throws IOException {
        assert parser.getCurrentToken() == JsonToken.START_OBJECT;
        Long id = null;
        String type = null;
        Long startId = null;
        Long endId = null;
        Map<String, Object> properties = Collections.emptyMap();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            switch (parser.getText()) {
                case "id":
                    parser.nextToken();
                    id = Long.valueOf(parser.getText());
                    break;
                case "startNode":
                    parser.nextToken();
                    startId = Long.valueOf(parser.getText());
                    break;
                case "endNode":
                    parser.nextToken();
                    endId = Long.valueOf(parser.getText());
                    break;
                case "type":
                    parser.nextToken();
                    type = parser.getText();
                    break;
                case "properties":
                    properties = JsonReader.readObject(parser);
                    break;
            }
        }
        return new Relationship(id, type, startId, endId, properties);
    }

    @Override
    public String toString() {
        return "Relationship(id=" + id + ",type=" + type + ",startId=" + startId + ",endId=" + endId + ",properties=" + properties + ")";
    }
}
