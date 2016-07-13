package io.github.jonestimd.neo4j.client.http;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public interface HttpResponse extends Closeable {
    String getHeader(String name);
    InputStream getEntityContent() throws IOException;
}
