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
package io.github.jonestimd.neo4j.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ApacheHttpDriverTest {
    private final String uri = "http://localhost";

    @Mock
    private CloseableHttpClient client;
    @Mock
    private HttpClientContext context;
    @Mock
    private CloseableHttpResponse httpResponse;

    private ApacheHttpDriver driver;
    @Captor
    private ArgumentCaptor<HttpPost> postCaptor;
    @Captor
    private ArgumentCaptor<HttpDelete> deleteCaptor;

    @Before
    public void injectMocks() throws Exception {
        driver = new ApacheHttpDriver(client, context);
    }

    @Test
    public void post() throws Exception {
        BasicHeader header = new BasicHeader("", "header-value");
        StringEntity responseEntity = new StringEntity("response entity");
        when(client.execute(any(HttpUriRequest.class), any(HttpClientContext.class))).thenReturn(httpResponse);
        when(httpResponse.getFirstHeader(anyString())).thenReturn(null, header);
        when(httpResponse.getEntity()).thenReturn(responseEntity);

        HttpResponse response = driver.post(uri, "json entity");

        verify(client).execute(postCaptor.capture(), same(context));
        HttpPost post = postCaptor.getValue();
        assertThat(post.getURI().toString()).isEqualTo(uri);
        assertThat(post.getEntity().getContentType().getValue()).isEqualTo(ContentType.APPLICATION_JSON.toString());
        assertThat(getContent(post.getEntity().getContent())).isEqualTo("json entity");
        assertThat(response.getHeader("header1")).isNull();
        assertThat(response.getHeader("header2")).isEqualTo(header.getValue());
        verify(httpResponse).getFirstHeader("header1");
        verify(httpResponse).getFirstHeader("header2");
        assertThat(getContent(response.getEntityContent())).isEqualTo("response entity");
        response.close();
        verify(httpResponse).close();
    }

    @Test
    public void delete() throws Exception {
        BasicHeader header = new BasicHeader("", "header-value");
        StringEntity responseEntity = new StringEntity("response entity");
        when(client.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getFirstHeader(anyString())).thenReturn(null, header);
        when(httpResponse.getEntity()).thenReturn(responseEntity);

        HttpResponse response = driver.delete(uri);

        verify(client).execute(deleteCaptor.capture());
        HttpDelete delete = deleteCaptor.getValue();
        assertThat(delete.getURI().toString()).isEqualTo(uri);
        assertThat(response.getHeader("header1")).isNull();
        assertThat(response.getHeader("header2")).isEqualTo(header.getValue());
        verify(httpResponse).getFirstHeader("header1");
        verify(httpResponse).getFirstHeader("header2");
        assertThat(getContent(response.getEntityContent())).isEqualTo("response entity");
        response.close();
        verify(httpResponse).close();
    }

    private String getContent(InputStream stream) throws IOException {
        StringBuilder buffer = new StringBuilder();
        int ch;
        while ((ch = stream.read()) >= 0) buffer.append((char) ch);
        return buffer.toString();
    }

    @Test
    public void createWithCredentials() throws Exception {
        ApacheHttpDriver driver = new ApacheHttpDriver("user", "password", "host", 9999);

        CloseableHttpClient client = getField(driver, "client");
        CredentialsProvider credentialsProvider = getField(client, "credentialsProvider");
        Credentials credentials = credentialsProvider.getCredentials(new AuthScope("host", 9999));
        assertThat(credentials.getUserPrincipal().getName()).isEqualTo("user");
        assertThat(credentials.getPassword()).isEqualTo("password");
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Object object, String fieldName) throws Exception {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(object);
    }
}