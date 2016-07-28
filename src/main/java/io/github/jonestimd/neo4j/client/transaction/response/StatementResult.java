package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * This class represents the result of a Cypher query.  The rows are selected sequentially using the {@link #next()}
 * method.  The get methods are used to retrieve the details for the current row.
 */
public class StatementResult {
    private final JsonParser parser;
    private final List<String> columns = new ArrayList<>();
    private ResultRow row;
    private boolean endOfResult = false;

    public StatementResult(JsonParser parser) throws IOException {
        this.parser = parser;
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            if (parser.getCurrentName().equals("columns")) {
                columns.addAll(JsonReader.readStrings(parser));
            }
            else if (parser.getCurrentName().equals("data")) {
                assert parser.nextToken() == JsonToken.START_ARRAY;
                break;
            }
            else {
                JsonReader.readNext(parser);
            }
        }
    }

    /**
     * Retrieve the next result row.
     * @return true if there is another row or false if there are no more rows.
     * @throws IOException
     */
    public boolean next() throws IOException {
        if (! endOfResult) {
            if (parser.nextToken() == JsonToken.START_OBJECT) {
                row = ResultRow.read(columns, parser);
                return true;
            }
            row = null;
            // skip additional properties in "results" array item
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                JsonReader.readNext(parser);
            }
            endOfResult = true;
        }
        return false;
    }

    /**
     * Get a column value for the current result row.
     * @param name the column name
     * @return the column value
     */
    public ResultColumn getColumn(String name) {
        return row.getColumn(name);
    }

    /**
     * Get a column metadata for the current result row.
     * @param name the column name
     * @return the column metadata
     */
    public List<ColumnMeta> getMeta(String name) {
        return row.getMeta(name);
    }

    /**
     * Get the graph nodes for the current result row.
     * @return a {@link List} of the nodes
     */
    public List<Node> getNodes() {
        return row.getNodes();
    }

    /**
     * Get the graph nodes for the current result row.
     * @return a {@link Map} of the nodes keyed by the graph IDs
     */
    public Map<Long, Node> getNodesById() {
        return row.getNodesById();
    }

    /**
     * Get the graph relationships for the current result row.
     * @return a {@link List} of the relationships
     */
    public List<Relationship> getRelationships() {
        return row.getRelationships();
    }
}
