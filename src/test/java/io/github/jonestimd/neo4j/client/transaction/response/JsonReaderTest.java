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

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;
import static org.fest.assertions.MapAssert.*;

public class JsonReaderTest {
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    public void readNextReadsString() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("\"abc\"".getBytes()));

        assertThat(JsonReader.readNext(parser)).isEqualTo("abc");
    }

    @Test
    public void readNextReadsLong() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("1234".getBytes()));

        assertThat(JsonReader.readNext(parser)).isEqualTo(1234L);
    }

    @Test
    public void readNextReadsBigDecimal() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("1234.5678".getBytes()));

        assertThat(JsonReader.readNext(parser)).isEqualTo(new BigDecimal("1234.5678"));
    }

    @Test
    public void readNextReadsBooleanTrue() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("true".getBytes()));

        assertThat(JsonReader.readNext(parser)).isEqualTo(true);
    }

    @Test
    public void readNextReadsBooleanFalse() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("false".getBytes()));

        assertThat(JsonReader.readNext(parser)).isEqualTo(false);
    }

    @Test
    public void readNextReadsNull() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("null".getBytes()));

        assertThat(JsonReader.readNext(parser)).isNull();
    }

    @Test
    public void readNextReadsArray() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("[null,\"abc\",123,false,true]".getBytes()));

        Object value = JsonReader.readNext(parser);

        assertThat(value).isInstanceOf(List.class);
        assertThat((List) value).containsExactly(null, "abc", 123L, false, true);
    }

    @Test
    public void readNextReadsMap() throws Exception {
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream("{\"string\":\"abc\",\"number\":123,\"boolean\":true}".getBytes()));

        Object value = JsonReader.readNext(parser);

        assertThat(value).isInstanceOf(Map.class);
        assertThat((Map) value).includes(entry("string", "abc"), entry("number", 123L), entry("boolean", true));
    }
}