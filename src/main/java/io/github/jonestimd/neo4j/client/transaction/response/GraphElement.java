package io.github.jonestimd.neo4j.client.transaction.response;

import java.util.Map;

public interface GraphElement {
    Long getId();
    Map<String, Object> getProperties();
}
