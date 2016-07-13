package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ResultRow {
    private final Map<String, ResultColumn> columns = new HashMap<>();
    private final Map<String, List<ColumnMeta>> meta = new HashMap<>();
    private List<Node> nodes = Collections.emptyList();
    private List<Relationship> relationships = Collections.emptyList();

    private void addColumns(List<String> columnNames, JsonParser parser) throws IOException {
        assert parser.nextToken() == JsonToken.START_ARRAY;
        for (String name : columnNames) {
            columns.put(name, new ResultColumn(parser));
        }
        assert parser.nextToken() == JsonToken.END_ARRAY;
    }

    private void addMeta(List<String> columnNames, JsonParser parser) throws IOException {
        assert parser.nextToken() == JsonToken.START_ARRAY;
        for (String name : columnNames) {
            meta.put(name, ColumnMeta.read(parser));
        }
        assert parser.nextToken() == JsonToken.END_ARRAY;
    }

    private void addGraph(JsonParser parser) throws IOException {
        assert parser.nextToken() == JsonToken.START_OBJECT;
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            switch (parser.getCurrentName()) {
                case "nodes":
                    nodes = Collections.unmodifiableList(JsonReader.readArray(parser, Node::read));
                    break;
                case "relationships":
                    relationships = Collections.unmodifiableList(JsonReader.readArray(parser, Relationship::read));
                    break;
            }
        }
    }

    public ResultColumn getColumn(String name) {
        return columns.get(name);
    }

    public List<ColumnMeta> getMeta(String name) {
        return meta.get(name);
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public Map<Long, Node> getNodesById() {
        return nodes.stream().collect(Collectors.toMap(Node::getId, Function.identity()));
    }

    public List<Relationship> getRelationships() {
        return relationships;
    }

    public static ResultRow read(List<String> columnNames, JsonParser parser) throws IOException {
        assert parser.getCurrentToken() == JsonToken.START_OBJECT;
        ResultRow row = new ResultRow();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            switch (parser.getCurrentName()) {
                case "row":
                    row.addColumns(columnNames, parser);
                    break;
                case "meta":
                    row.addMeta(columnNames, parser);
                    break;
                case "graph":
                    row.addGraph(parser);
                    break;
            }
        }
        return row;
    }
}
