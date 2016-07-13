package io.github.jonestimd.neo4j.client.http;

import java.io.IOException;

public interface HttpDriver {
    HttpResponse post(String uri, String jsonEntity) throws IOException;

    HttpResponse delete(String uri) throws IOException;
}
