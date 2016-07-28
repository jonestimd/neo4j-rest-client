package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParser;

/**
 * This class represents a column value in a row of a query result.  The type of the column value depends on the
 * selected value in the query and can be retrieved as
 * <ul>
 *     <li>a {@code String}</li>
 *     <li>a {@code Number}</li>
 *     <li>a {@code boolean}</li>
 *     <li>a {@code Map} of node or relationship properties</li>
 *     <li>a {@code List} of one of the other types</li>
 * </ul>
 */
public class ResultColumn {
    private final Object value;

    public ResultColumn(JsonParser parser) throws IOException {
        this.value = JsonReader.readNext(parser);
    }

    /**
     * @return the column value as a {@code String}
     */
    public Optional<String> getString() {
        return Optional.of((String) value);
    }

    /**
     * @return the column value as a {@code Number}
     */
    public Optional<Number> getNumber() {
        return Optional.of((Number) value);
    }

    /**
     * @return the column value as a {@code Boolean}
     */
    public Optional<Boolean> getBoolean() {
        return Optional.of((Boolean) value);
    }

    /**
     * @return the column value as properties of a graph element
     */
    @SuppressWarnings("unchecked")
    public Map<String, ?> getProperties() {
        return (Map<String, ?>) value;
    }

    /**
     * @param <T> the type of the list elements
     * @return the column value as a list
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getList() {
        return value == null ? Collections.emptyList() : (List<T>) value;
    }
}
