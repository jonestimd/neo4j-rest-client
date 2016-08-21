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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Utility class for parsing a JSON stream.
 */
public class JsonReader {
    /**
     * Read the next object from a stream.  Depending on the next token in the stream, the returned value will be
     * one of
     * <ul>
     *     <li>a {@code Map<String,Object>}</li>
     *     <li>a {@code List<Object>}</li>
     *     <li>a {@code String}</li>
     *     <li>a {@code Long}</li>
     *     <li>a {@code BigDecimal}</li>
     *     <li>a {@code Boolean}</li>
     *     <li>a {@code null}</li>
     * </ul>
     * @param parser the JSON stream parser
     * @throws IOException
     */
    public static Object readNext(JsonParser parser) throws IOException {
        parser.nextToken();
        return getValue(parser);
    }

    private static Object getValue(JsonParser parser) throws IOException {
        switch (parser.getCurrentToken()) {
            case START_ARRAY: return getArray(parser, JsonReader::getValue);
            case START_OBJECT: return getObject(parser);
            case VALUE_STRING: return parser.getText();
            case VALUE_NUMBER_INT: return parser.getLongValue();
            case VALUE_NUMBER_FLOAT: return parser.getDecimalValue();
            case VALUE_TRUE: return Boolean.TRUE;
            case VALUE_FALSE: return Boolean.FALSE;
            case VALUE_NULL: return null;
            default: throw new ParseResponseException(parser.getCurrentLocation());
        }
    }

    /**
     * Read an array from a stream.
     * @param parser the JSON stream parser
     * @param decoder a function to read an array element
     * @param <T> the type of the array elements
     * @return a {@code List} containing the array elements
     * @throws IOException
     */
    public static <T> List<T> readArray(JsonParser parser, JsonDecoder<T> decoder) throws IOException {
        checkNextToken(parser, JsonToken.START_ARRAY);
        return getArray(parser, decoder);
    }

    /**
     * Read an array of string values from a stream.
     * @param parser the JSON stream parser
     * @throws IOException
     */
    public static List<String> readStrings(JsonParser parser) throws IOException {
        return readArray(parser, JsonParser::getText);
    }

    /**
     * Read an array of JSON objects from a stream.
     * @param parser the JSON stream parser
     * @return a {@code Map} of name/value pairs for each JSON object in the array
     * @throws IOException
     */
    public static List<Map<String, Object>> readObjects(JsonParser parser) throws IOException {
        return readArray(parser, JsonReader::getObject);
    }

    private static <T> List<T> getArray(JsonParser parser, JsonDecoder<T> decoder) throws IOException {
        List<T> result = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            result.add(decoder.apply(parser));
        }
        return result;
    }

    /**
     * Read a JSON object from a stream.
     * @param parser the JSON stream parser
     * @return a {@code Map} of name/value pairs for the object
     * @throws IOException
     */
    public static Map<String, Object> readObject(JsonParser parser) throws IOException {
        checkNextToken(parser, JsonToken.START_OBJECT);
        return getObject(parser);
    }

    private static Map<String, Object> getObject(JsonParser parser) throws IOException {
        Map<String, Object> result = new HashMap<>();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            result.put(parser.getText(), readNext(parser));
        }
        return result;
    }

    public static void checkToken(JsonParser parser, JsonToken expected) {
        if (parser.getCurrentToken() != expected) throw new ParseResponseException(parser.getCurrentLocation());
    }

    public static void checkNextToken(JsonParser parser, JsonToken expected) throws IOException {
        if (parser.nextToken() != expected) throw new ParseResponseException(parser.getCurrentLocation());
    }

    public static Long readLong(JsonParser parser) throws IOException {
        parser.nextToken();
        return Long.valueOf(parser.getText());
    }

    /**
     * A functional interface for reading a value from a JSON parser.
     * @param <R> the type of the value to read from the stream
     */
    public interface JsonDecoder<R> {
        R apply(JsonParser input) throws IOException;
    }
}
