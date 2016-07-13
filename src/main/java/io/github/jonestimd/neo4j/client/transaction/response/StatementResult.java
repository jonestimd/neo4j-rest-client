package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

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

    public ResultColumn getColumn(String name) {
        return row.getColumn(name);
    }

    public List<ColumnMeta> getMeta(String name) {
        return row.getMeta(name);
    }

    public List<Node> getNodes() {
        return row.getNodes();
    }

    public Map<Long, Node> getNodesById() {
        return row.getNodesById();
    }

    public List<Relationship> getRelationships() {
        return row.getRelationships();
    }
}
