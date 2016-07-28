package io.github.jonestimd.neo4j.client.transaction.response;

/**
 * This class represents an error response for a Cypher query.
 */
public class StatementException extends RuntimeException {
    private final String code;

    public StatementException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
