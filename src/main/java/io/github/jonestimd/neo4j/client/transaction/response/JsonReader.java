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
        return getValue(parser.nextToken(), parser);
    }

    private static Object getValue(JsonToken event, JsonParser parser) throws IOException {
        switch (event) {
            case START_ARRAY: return getArray(parser);
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
     * @param reader a function to read an array element
     * @param <T> the type of the array elements
     * @return a {@code List} containing the array elements
     * @throws IOException
     */
    public static <T> List<T> readArray(JsonParser parser, JsonDecoder<T> reader) throws IOException {
        assert parser.nextToken() == JsonToken.START_ARRAY;
        List<T> result = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            result.add(reader.apply(parser));
        }
        return result;
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

    private static List<Object> getArray(JsonParser parser) throws IOException {
        List<Object> result = new ArrayList<>();
        JsonToken event;
        while ((event = parser.nextToken()) != JsonToken.END_ARRAY) {
            result.add(getValue(event, parser));
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
        assert parser.nextToken() == JsonToken.START_OBJECT;
        return getObject(parser);
    }

    private static Map<String, Object> getObject(JsonParser parser) throws IOException {
        Map<String, Object> result = new HashMap<>();
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            result.put(parser.getText(), readNext(parser));
        }
        return result;
    }

    /**
     * A functional interface for reading a value from a JSON parser.
     * @param <R> the type of the value to read from the stream
     */
    public interface JsonDecoder<R> {
        R apply(JsonParser input) throws IOException;
    }
}
