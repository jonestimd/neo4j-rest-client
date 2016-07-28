package io.github.jonestimd.neo4j.client.transaction;

import java.io.IOException;
import java.util.Timer;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.JsonFactory;
import io.github.jonestimd.neo4j.client.http.HttpDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used to create transactions and perform tasks within transactions.  When a transaction is created,
 * it is associated with the current thread, so nested calls to {@link #doInTransaction(TransactionCallback)} on the
 * same thread will use the same transaction.  If {@link #doInTransaction(TransactionCallback)} throws an exception
 * then the transaction is rolled back.  Otherwise, the transaction is committed when the outermost
 * {@link #doInTransaction(TransactionCallback)} returns.
 */
public class TransactionManager {
    private static final ThreadLocal<Transaction> TRANSACTION_HOLDER = new ThreadLocal<>();

    private final Supplier<Transaction> transactionFactory;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a transaction manager that uses the supplied parameters to create transactions.  The driver and URL
     * are required and the remaining parameters are optional.
     * @param httpDriver the HTTP driver to use for requests
     * @param baseUrl the base URL for the Neo4j transaction REST API
     * @param jsonFactory factory for creating generators and parsers
     * @param timer the timer to use for scheduling the keep alive task
     * @param keepAliveMs the period of the keep alive requests in milliseconds
     */
    public TransactionManager(HttpDriver httpDriver, String baseUrl, JsonFactory jsonFactory, Timer timer, long keepAliveMs) {
        this(() -> new Transaction(httpDriver, baseUrl, jsonFactory, timer, keepAliveMs));
    }

    /**
     * Create a transaction manager using the supplied factory to create transactions.
     * @param transactionFactory a factory that creates new transactions
     */
    public TransactionManager(Supplier<Transaction> transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    /**
     * Get the transaction associated with the current thread.
     * @return the transaction or {@code null} is there is no open transaction
     */
    public static Transaction getTransaction() {
        return TRANSACTION_HOLDER.get();
    }

    /**
     * Run the {@code callback} in a transaction.  If there is already a transaction associated with the current thread
     * then that transaction is used.  Otherwise, a new transaction is created, passed to the {@code callback} and
     * committed or rolled back after the {@code callback} completes.
     * @param callback the task to perform in the transaction
     * @param <T> the result type of the {@code callback}
     * @return the result of {@code callback}
     * @throws Exception
     */
    public <T> T doInTransaction(TransactionCallback<T> callback) throws Exception {
        Transaction transaction = TRANSACTION_HOLDER.get();
        if (transaction == null) {
            transaction = transactionFactory.get();
            TRANSACTION_HOLDER.set(transaction);
            try {
                T result = callback.apply(transaction);
                if (!transaction.isComplete()) {
                    transaction.commit();
                }
                return result;
            } catch (Throwable ex) {
                try {
                    if (!transaction.isComplete()) {
                        transaction.rollback();
                    }
                } catch (IOException e) {
                    logger.error("transaction rollback failed", ex);
                }
                throw ex;
            } finally {
                TRANSACTION_HOLDER.remove();
            }
        }
        else return callback.apply(transaction);
    }
}
