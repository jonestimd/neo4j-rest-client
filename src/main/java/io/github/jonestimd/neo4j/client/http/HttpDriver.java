package io.github.jonestimd.neo4j.client.http;

import java.io.IOException;

import io.github.jonestimd.neo4j.client.transaction.Transaction;

/**
 * This interface is used by {@link Transaction} to make HTTP requests.
 */
public interface HttpDriver {
    /**
     * Send an HTTP Post to a service.
     * @param uri the service URI
     * @param jsonEntity the request body
     * @return the service response
     * @throws IOException
     */
    HttpResponse post(String uri, String jsonEntity) throws IOException;

    /**
     * Send an HTTP Delete to a service.
     * @param uri the service URI
     * @return the service response
     * @throws IOException
     */
    HttpResponse delete(String uri) throws IOException;
}
