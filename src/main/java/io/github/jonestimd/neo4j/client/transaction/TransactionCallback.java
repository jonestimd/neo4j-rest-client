package io.github.jonestimd.neo4j.client.transaction;

/**
 * This functional interface is used with {@link TransactionManager} to perform tasks in a transaction.
 * @param <T> the result type of the task
 * @see {@link TransactionManager#doInTransaction(TransactionCallback)}
 */
public interface TransactionCallback<T> {
    /**
     * Called by {@link TransactionManager}.
     * @param transaction the current transaction.
     * @return the result of the task
     * @throws Exception
     */
    T apply(Transaction transaction) throws Exception;
}
