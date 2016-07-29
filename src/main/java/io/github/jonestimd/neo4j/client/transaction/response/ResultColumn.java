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
