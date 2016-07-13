package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class Response {
    public static final Response EMPTY = new Response();
    private final JsonParser parser;
    private StatementResult result;
    private boolean endOfResponse = false;

    private Response() {
        this.parser = null;
        this.endOfResponse = true;
    }

    public Response(JsonParser parser) throws StatementException, IOException {
        this.parser = parser;
        assert parser.nextToken() == JsonToken.START_OBJECT;
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            if (parser.getCurrentName().equals("results")) {
                assert parser.nextToken() == JsonToken.START_ARRAY;
                break;
            }
            else if (parser.getCurrentName().equals("errors")) readErrors();
            else JsonReader.readNext(parser);
        }
        endOfResponse = parser.getCurrentToken() == JsonToken.END_OBJECT;
    }

    public boolean next() throws StatementException, IOException {
        if (! endOfResponse) {
            if (result != null) {
                while (result.next());
            }
            switch (parser.nextToken()) {
                case START_OBJECT:
                    result = new StatementResult(parser);
                    return true;
                case END_ARRAY:
                    while (parser.nextToken() == JsonToken.FIELD_NAME) {
                        if (parser.getCurrentName().equals("errors")) readErrors();
                    }
            }
            result = null;
            endOfResponse = true;
        }
        return false;
    }

    private void readErrors() throws IOException {
        List<Map<String, Object>> errors = JsonReader.readObjects(parser);
        if (! errors.isEmpty()) {
            Map<String, Object> error = errors.get(0);
            throw new StatementException((String) error.get("code"), (String) error.get("message"));
        }
    }

    public StatementResult getResult() {
        return result;
    }
}
