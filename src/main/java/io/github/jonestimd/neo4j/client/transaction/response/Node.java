package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * This class represents a graph node in a query result.
 */
public class Node implements GraphElement {
    private final Long id;
    private final Set<String> labels;
    private final Map<String, Object> properties;

    public Node() {
        this(null, Collections.emptySet(), Collections.emptyMap());
    }

    public Node(Long id, Set<String> labels, Map<String, Object> properties) {
        this.id = id;
        this.labels = Collections.unmodifiableSet(labels);
        this.properties = Collections.unmodifiableMap(properties);
    }

    /**
     * Get the graph ID of this node.
     */
    @Override
    public Long getId() {
        return id;
    }

    /**
     * Get the labels of this node.
     */
    public Set<String> getLabels() {
        return labels;
    }

    /**
     * Get the properties attached to this node.
     */
    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    public static Node read(JsonParser parser) throws IOException {
        assert parser.getCurrentToken() == JsonToken.START_OBJECT;
        Long id = null;
        Set<String> labels = new HashSet<>();
        Map<String, Object> properties = Collections.emptyMap();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            switch (parser.getCurrentName()) {
                case "id":
                    id = Long.valueOf(parser.nextTextValue());
                    break;
                case "labels":
                    assert parser.nextToken() == JsonToken.START_ARRAY;
                    while (parser.nextToken() != JsonToken.END_ARRAY) labels.add(parser.getText());
                    break;
                case "properties":
                    properties = JsonReader.readObject(parser);
                    break;
            }
        }
        return new Node(id, labels, properties);
    }

    @Override
    public String toString() {
        return "Node(id=" + id + ",labels=" + labels + ",properties=" + properties + ")";
    }
}
