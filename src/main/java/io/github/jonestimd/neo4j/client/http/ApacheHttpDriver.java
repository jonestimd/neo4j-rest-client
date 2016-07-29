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

import org.apache.http.Header;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * This class implements {@link HttpDriver} using the Apache HTTP client library.
 */
public class ApacheHttpDriver implements HttpDriver {
    private final CloseableHttpClient client;
    private final HttpClientContext clientContext;

    public ApacheHttpDriver(String userName, String password, String host, int port) {
        this(createClient(credentialsProvider(userName, password, host, port)), null);
    }

    public ApacheHttpDriver(CloseableHttpClient client, HttpClientContext clientContext) {
        this.client = client;
        this.clientContext = clientContext;
    }

    private static CloseableHttpClient createClient(CredentialsProvider credentialsProvider) {
        return HttpClients.custom().setDefaultCredentialsProvider(credentialsProvider).build();
    }

    private static BasicCredentialsProvider credentialsProvider(String userName, String password, String host, int port) {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(host, port),
                new UsernamePasswordCredentials(userName, password));
        return credentialsProvider;
    }

    public HttpResponse post(String uri, String jsonEntity) throws IOException {
        HttpPost post = new HttpPost(uri);
        StringEntity entity = new StringEntity(jsonEntity);
        entity.setContentType(ContentType.APPLICATION_JSON.toString());
        post.setEntity(entity);
        return new ResponseAdapter(client.execute(post, clientContext));
    }

    public HttpResponse delete(String uri) throws IOException {
        return new ResponseAdapter(client.execute(new HttpDelete(uri)));
    }

    private static class ResponseAdapter implements HttpResponse {
        private final CloseableHttpResponse response;

        public ResponseAdapter(CloseableHttpResponse response) {
            this.response = response;
        }

        @Override
        public String getHeader(String name) {
            Header header = response.getFirstHeader(name);
            return header == null ? null : header.getValue();
        }

        @Override
        public InputStream getEntityContent() throws IOException {
            return response.getEntity().getContent();
        }

        @Override
        public void close() throws IOException {
            response.close();
        }
    }
}
