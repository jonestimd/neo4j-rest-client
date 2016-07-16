package io.github.jonestimd.neo4j.client.transaction;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Timer;

import io.github.jonestimd.neo4j.client.http.HttpDriver;
import io.github.jonestimd.neo4j.client.http.HttpResponse;
import io.github.jonestimd.neo4j.client.transaction.request.Statement;
import io.github.jonestimd.neo4j.client.transaction.response.Response;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static java.util.Collections.*;
import static org.fest.assertions.Assertions.*;
import static org.mockito.Mockito.*;

public class TransactionTest {
    private static final String BASE_URL = "http://localhost:7474/db/data/transaction";
    private static final String RESPONSE_JSON =
            "{\"results\":[{\"columns\":[],\"data\":[]}],\"errors\":[]}";
    public static final String LOCATION_HEADER = "Location";
    public static final String EMPTY_RESPONSE_JSON = "{\"results\":[],\"errors\":[]}";
    public static final String ERROR_RESPONSE_JSON = "{\"results\":[],\"errors\":[{\"code\":\"syntax error\",\"message\":\"error message\"}]}";
    public static final String STATEMENTS_JSON = "{\"statements\":[{\"statement\":\"cypher query\"," +
            "\"parameters\":{\"param\":\"value\"}," +
            "\"resultDataContents\":[\"row\",\"graph\"]}]}";
    public static final String CYPHER_QUERY = "cypher query";
    public static final Map<String, String> PARAM_MAP = singletonMap("param", "value");
    private HttpDriver httpDriver = mock(HttpDriver.class);
    private Transaction transaction = new Transaction(httpDriver, BASE_URL);
    private HttpResponse httpResponse = mock(HttpResponse.class);
    private Answer<InputStream> entityAnswer = (invocation) -> new ByteArrayInputStream(RESPONSE_JSON.getBytes());
    private Answer<InputStream> emptyAnswer = (invocation) -> new ByteArrayInputStream(EMPTY_RESPONSE_JSON.getBytes());

    @Test
    public void executeNoStatementsReturnsEmptyResult() throws Exception {
        Response response = transaction.execute();

        assertThat(response.next()).isFalse();
        verifyZeroInteractions(httpDriver);
    }

    @Test
    public void firstExecutePostsStatementsToBaseUrl() throws Exception {
        String query = "cypher query";
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getEntityContent()).thenAnswer(entityAnswer);

        Response response = transaction.execute(new Statement(query, PARAM_MAP));

        assertThat(response.next()).isTrue();
        assertThat(response.getResult().next()).isFalse();
        verify(httpDriver).post(BASE_URL, STATEMENTS_JSON);
        verify(httpResponse).getHeader(LOCATION_HEADER);
        verify(httpResponse).getEntityContent();
        verify(httpResponse).close();
    }

    @Test
    public void executePostsStatementsToLocationUrl() throws Exception {
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent())
                .thenReturn(new ByteArrayInputStream(EMPTY_RESPONSE_JSON.getBytes()))
                .thenReturn(new ByteArrayInputStream(RESPONSE_JSON.getBytes()));
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));

        Response response = transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));

        assertThat(response.next()).isTrue();
        assertThat(response.getResult().next()).isFalse();
        verify(httpDriver).post(BASE_URL, STATEMENTS_JSON);
        verify(httpDriver).post(BASE_URL + "/1", STATEMENTS_JSON);
        verify(httpResponse, times(2)).getHeader(LOCATION_HEADER);
        verify(httpResponse, times(2)).getEntityContent();
        verify(httpResponse, times(2)).close();
    }

    @Test
    public void rollbackUnusedTransactionReturnsEmptyResponse() throws Exception {
        Response response = transaction.rollback();

        assertThat(response.next()).isFalse();
    }

    @Test
    public void rollbackDeletesLocationUrl() throws Exception {
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpDriver.delete(anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent()).thenAnswer(emptyAnswer);
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));

        Response response = transaction.rollback();

        assertThat(response.next()).isFalse();
        verify(httpDriver).post(BASE_URL, STATEMENTS_JSON);
        verify(httpDriver).delete(BASE_URL + "/1");
        verify(httpResponse).getHeader(LOCATION_HEADER);
        verify(httpResponse, times(2)).getEntityContent();
        verify(httpResponse, times(2)).close();
    }

    @Test(expected = IllegalStateException.class)
    public void rollbackThrowsExceptionWhenTransactionIsComplete() throws Exception {
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpDriver.delete(anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent()).thenAnswer(emptyAnswer);
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));
        transaction.rollback();

        transaction.rollback();
    }

    @Test
    public void commitReturnsEmptyResponseForEmptyTransaction() throws Exception {
        Response response = transaction.commit();

        assertThat(response.next()).isFalse();
    }

    @Test
    public void commitPostsToBaseUrl() throws Exception {
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent()).thenAnswer(entityAnswer);

        Response response = transaction.commit(new Statement(CYPHER_QUERY, PARAM_MAP));

        assertThat(response.next()).isTrue();
        verify(httpDriver).post(BASE_URL + "/commit", STATEMENTS_JSON);
        verify(httpResponse).getHeader(LOCATION_HEADER);
        verify(httpResponse).getEntityContent();
        verify(httpResponse).close();
    }

    @Test
    public void commitPostsToLocationUrl() throws Exception {
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent())
                .thenReturn(new ByteArrayInputStream(RESPONSE_JSON.getBytes()))
                .thenReturn(new ByteArrayInputStream(EMPTY_RESPONSE_JSON.getBytes()));
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));

        Response response = transaction.commit();

        assertThat(response.next()).isFalse();
        verify(httpDriver).post(BASE_URL, STATEMENTS_JSON);
        verify(httpDriver).post(BASE_URL + "/1/commit", "{\"statements\":[]}");
        verify(httpResponse, times(2)).getHeader(LOCATION_HEADER);
        verify(httpResponse, times(2)).getEntityContent();
        verify(httpResponse, times(2)).close();
    }

    @Test(expected = IllegalStateException.class)
    public void commitThrowsExceptionWhenTransactionIsComplete() throws Exception {
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent()).thenAnswer(entityAnswer);
        transaction.commit(new Statement(CYPHER_QUERY, PARAM_MAP));

        transaction.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void executeThrowsExceptionWhenTransactionIsComplete() throws Exception {
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent()).thenAnswer(entityAnswer);
        transaction.commit(new Statement(CYPHER_QUERY, PARAM_MAP));

        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));
    }

    @Test
    public void pingsTransactionUrlUntilCompleted() throws Exception {
        Transaction transaction = new Transaction(httpDriver, BASE_URL, new Timer(true), 30L);
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpDriver.delete(anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent()).thenAnswer(entityAnswer);
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));

        Thread.sleep(40L);
        transaction.rollback();

        Thread.sleep(50L);
        verify(httpDriver).post(BASE_URL, STATEMENTS_JSON);
        verify(httpDriver).post(BASE_URL + "/1", "{\"statements\":[]}");
        verify(httpResponse, times(3)).close();
    }

    @Test
    public void waitsToPingAfterRequest() throws Exception {
        Transaction transaction = new Transaction(httpDriver, BASE_URL, new Timer(true), 30L);
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent()).thenAnswer(entityAnswer);
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));

        Thread.sleep(20L);
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));
        Thread.sleep(45L);
        transaction.commit();

        Thread.sleep(50L);
        verify(httpDriver).post(BASE_URL, STATEMENTS_JSON);
        verify(httpDriver).post(BASE_URL + "/1", STATEMENTS_JSON);
        verify(httpDriver).post(BASE_URL + "/1", "{\"statements\":[]}");
        verify(httpResponse, times(4)).close();
    }

    @Test
    public void continuesPingingAfterErrorOnPingRequest() throws Exception {
        Transaction transaction = new Transaction(httpDriver, BASE_URL, new Timer(true), 30L);
        when(httpDriver.post(anyString(), anyString())).thenReturn(httpResponse);
        when(httpDriver.delete(anyString())).thenReturn(httpResponse);
        when(httpResponse.getHeader(LOCATION_HEADER)).thenReturn(BASE_URL + "/1");
        when(httpResponse.getEntityContent())
                .thenReturn(new ByteArrayInputStream(RESPONSE_JSON.getBytes()))
                .thenReturn(new ByteArrayInputStream(ERROR_RESPONSE_JSON.getBytes()))
                .thenAnswer(entityAnswer);
        transaction.execute(new Statement(CYPHER_QUERY, PARAM_MAP));

        Thread.sleep(75L);
        transaction.rollback();

        Thread.sleep(50L);
        verify(httpDriver).post(BASE_URL, STATEMENTS_JSON);
        verify(httpDriver, times(2)).post(BASE_URL + "/1", "{\"statements\":[]}");
        verify(httpResponse, times(4)).close();
    }
}