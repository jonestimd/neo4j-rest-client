package io.github.jonestimd.neo4j.client.transaction;

public interface TransactionCallback<T> {
    T apply(Transaction transaction) throws Exception;
}
