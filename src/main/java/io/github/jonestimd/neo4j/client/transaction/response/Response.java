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
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import static io.github.jonestimd.neo4j.client.transaction.response.JsonReader.*;

/**
 * This class represents the results of a group of Cypher queries.  The query results are selected sequentially using
 * the {@link #next()} method.  The {@link #getResult()} method is used to retrieve the current query result.
 */
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
        checkNextToken(parser, JsonToken.START_OBJECT);
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            if (parser.getCurrentName().equals("results")) {
                checkNextToken(parser, JsonToken.START_ARRAY);
                break;
            }
            else if (parser.getCurrentName().equals("errors")) readErrors();
            else readNext(parser);
        }
        endOfResponse = parser.getCurrentToken() == JsonToken.END_OBJECT;
    }

    /**
     * Retrieve the next query result.
     * @return true if there is another result or false if there are no more results.
     * @throws IOException
     * @throws StatementException if the query resulted in an error
     */
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
                    break;
                default: throw new ParseResponseException(parser.getCurrentLocation());
            }
            result = null;
            endOfResponse = true;
        }
        return false;
    }

    private void readErrors() throws IOException {
        List<Map<String, Object>> errors = readObjects(parser);
        if (! errors.isEmpty()) {
            Map<String, Object> error = errors.get(0);
            throw new StatementException((String) error.get("code"), (String) error.get("message"));
        }
    }

    /**
     * Get the current query result.
     */
    public StatementResult getResult() {
        return result;
    }
}
