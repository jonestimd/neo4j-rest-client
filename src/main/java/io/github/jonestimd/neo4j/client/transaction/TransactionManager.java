package io.github.jonestimd.neo4j.client.transaction;

import java.io.IOException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionManager {
    private static final ThreadLocal<Transaction> TRANSACTION_HOLDER = new ThreadLocal<>();

    private final Supplier<Transaction> transactionFactory;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public TransactionManager(Supplier<Transaction> transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    public static Transaction getTransaction() {
        return TRANSACTION_HOLDER.get();
    }

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
