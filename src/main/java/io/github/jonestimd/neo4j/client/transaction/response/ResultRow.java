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

/**
 * This class represents a row of a query result.  It contains the column values, column metadata, nodes and
 * relationships for the result row.
 */
public class ResultRow {
    private final Map<String, ResultColumn> columns = new HashMap<>();
    private final Map<String, List<ColumnMeta>> meta = new HashMap<>();
    private List<Node> nodes = Collections.emptyList();
    private List<Relationship> relationships = Collections.emptyList();

    /**
     * Get the value of a column in the current row.
     * @param name the column name
     */
    public ResultColumn getColumn(String name) {
        return columns.get(name);
    }

    /**
     * Get the metadata of a column in the current row.
     * @param name the column name
     */
    public List<ColumnMeta> getMeta(String name) {
        return meta.get(name);
    }

    /**
     * Get the list of graph nodes for the current row.
     */
    public List<Node> getNodes() {
        return nodes;
    }

    /**
     * Get a map of the graph nodes for the current row.  Can be used to look up the start and end nodes of a relationship.
     * @return a {@link Map} of nodes keyed by the graph IDs.
     */
    public Map<Long, Node> getNodesById() {
        return nodes.stream().collect(Collectors.toMap(Node::getId, Function.identity()));
    }

    /**
     * Get the list of graph relationships for the current row.
     */
    public List<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Read the next result row from a JSON stream.
     * @param columnNames the query result column names
     * @param parser the JSON parser for the stream
     * @return a row of the query result
     * @throws IOException
     */
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
}
