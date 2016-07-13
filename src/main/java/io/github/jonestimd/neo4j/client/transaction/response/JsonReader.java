package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JsonReader {
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

    public static <T> List<T> readArray(JsonParser parser, FailableFunction<JsonParser, T> reader) throws IOException {
        assert parser.nextToken() == JsonToken.START_ARRAY;
        List<T> result = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            result.add(reader.apply(parser));
        }
        return result;
    }

    public static List<String> readStrings(JsonParser parser) throws IOException {
        return readArray(parser, JsonParser::getText);
    }

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

    public interface FailableFunction<T, R> {
        R apply(T input) throws IOException;
    }
}
