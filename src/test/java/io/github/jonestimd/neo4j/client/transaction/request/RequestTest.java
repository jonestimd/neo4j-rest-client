package io.github.jonestimd.neo4j.client.transaction.request;

import com.fasterxml.jackson.core.JsonFactory;
import org.junit.Test;

import static java.util.Collections.*;
import static org.fest.assertions.Assertions.*;

public class RequestTest {
    public static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Test(expected = UnsupportedOperationException.class)
    public void statementsAreUnmodifiable() throws Exception {
        new Request().getStatements().add(new Statement(null, null));
    }

    @Test
    public void toJsonIncludesAllStatements() throws Exception {
        Request request = new Request(
                new Statement("query1", singletonMap("param1", "value1"), true, ResultType.Graph),
                new Statement("query2", singletonMap("param2", "value2"), false, ResultType.Row));

        String json = request.toJson(JSON_FACTORY);

        assertThat(json).isEqualTo("{\"statements\":[" +
                "{\"statement\":\"query1\",\"parameters\":{\"param1\":\"value1\"},\"includeStats\":true,\"resultDataContents\":[\"graph\"]}," +
                "{\"statement\":\"query2\",\"parameters\":{\"param2\":\"value2\"},\"resultDataContents\":[\"row\"]}]}");
    }
}