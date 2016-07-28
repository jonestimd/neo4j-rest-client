package io.github.jonestimd.neo4j.client.transaction.response;

import java.util.Map;

/**
 * Common interface for nodes and relationships returned as query results.
 */
public interface GraphElement {
    Long getId();
    Map<String, Object> getProperties();
}
