package io.github.jonestimd.neo4j.client.http;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import io.github.jonestimd.neo4j.client.transaction.Transaction;

/**
 * This interface is used by {@link Transaction} to extract information from an HTTP service response.
 */
public interface HttpResponse extends Closeable {
    /**
     * Get a header value from the response.
     * @param name the header name
     * @return the header value
     */
    String getHeader(String name);

    /**
     * Get the service response body as a stream.
     * @return a stream for reading the response body
     * @throws IOException
     */
    InputStream getEntityContent() throws IOException;
}
