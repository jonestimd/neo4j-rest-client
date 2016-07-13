package io.github.jonestimd.neo4j.client.transaction.response;

import java.io.ByteArrayInputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import junit.framework.Assert;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;

public class ResponseTest {
    private final JsonFactory jsonFactory = new JsonFactory();

    @Test
    public void emptyResponse() throws Exception {
        String json = "{}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));

        Response response = new Response(parser);

        assertThat(response.next()).isFalse();
    }

    @Test
    public void errorFirstResponse() throws Exception {
        String json = "{\"errors\":[{\"code\":\"Code\",\"message\":\"Message\"}],\"results\":[]}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));

        try {
            new Response(parser);
            Assert.fail("expected exception");
        } catch (StatementException ex) {
            assertThat(ex.getCode()).isEqualTo("Code");
            assertThat(ex.getMessage()).isEqualTo("Message");
        }
    }

    @Test
    public void errorLastResponse() throws Exception {
        String json = "{\"ignored\":{},\"results\":[],\"errors\":[{\"code\":\"Code\",\"message\":\"Message\"}]}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        Response response = new Response(parser);

        try {
            response.next();
            Assert.fail("expected exception");
        } catch (StatementException ex) {
            assertThat(ex.getCode()).isEqualTo("Code");
            assertThat(ex.getMessage()).isEqualTo("Message");
        }
    }

    @Test
    public void emptyResultResponse() throws Exception {
        String json = "{\"ignored\":{},\"results\":[{\"columns\":[],\"data\":[]}],\"errors\":[]}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        Response response = new Response(parser);

        assertThat(response.next()).isTrue();

        assertThat(response.next()).isFalse();
    }

    @Test
    public void nextSkipsToNextStatementResult() throws Exception {
        String json = "{\"ignored\":{},\"results\":[{\"columns\":[\"c1\"],\"data\":[{\"row\":[123],\"meta\":[null]}]}],\"errors\":[]}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        Response response = new Response(parser);
        assertThat(response.next()).isTrue();

        assertThat(response.next()).isFalse();
    }

    @Test
    public void getResult() throws Exception {
        String json = "{\"ignored\":{},\"results\":[{\"columns\":[\"c1\"],\"data\":[{\"row\":[123],\"meta\":[null]}]}],\"errors\":[]}";
        JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
        Response response = new Response(parser);
        assertThat(response.next()).isTrue();

        assertThat(response.getResult().next()).isTrue();

        assertThat(response.next()).isFalse();
        assertThat(response.getResult()).isNull();
        assertThat(response.next()).isFalse();
    }
}