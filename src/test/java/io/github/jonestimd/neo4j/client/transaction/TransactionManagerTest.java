package io.github.jonestimd.neo4j.client.transaction;

import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;
import org.junit.Test;

import static org.fest.assertions.Assertions.*;
import static org.mockito.Mockito.*;

public class TransactionManagerTest {
    @SuppressWarnings("unchecked")
    private TransactionCallback<Long> callback = mock(TransactionCallback.class);
    private Transaction transaction = mock(Transaction.class);

    private TransactionManager transactionManager = new TransactionManager(() -> transaction);

    @Test
    public void doInTransactionCommitsIfNotComplete() throws Exception {
        long result = new Random().nextLong();
        when(callback.apply(transaction)).thenAnswer(invocation -> {
            assertThat(invocation.getArguments()[0]).isSameAs(transaction);
            assertThat(TransactionManager.getTransaction()).isSameAs(transaction);
            return result;
        });
        when(transaction.isComplete()).thenReturn(false);

        assertThat(transactionManager.doInTransaction(callback)).isEqualTo(result);

        verify(transaction).commit();
        verify(transaction, never()).rollback();
        verify(callback).apply(transaction);
        assertThat(TransactionManager.getTransaction()).isNull();
    }

    @Test
    public void doInTransactionDoesNotCommitIfComplete() throws Exception {
        long result = new Random().nextLong();
        when(callback.apply(transaction)).thenAnswer(invocation -> {
            assertThat(invocation.getArguments()[0]).isSameAs(transaction);
            assertThat(TransactionManager.getTransaction()).isSameAs(transaction);
            return result;
        });
        when(transaction.isComplete()).thenReturn(true);

        assertThat(transactionManager.doInTransaction(callback)).isEqualTo(result);

        verify(transaction, never()).commit();
        verify(transaction, never()).rollback();
        verify(callback).apply(transaction);
        assertThat(TransactionManager.getTransaction()).isNull();
    }

    @Test
    public void doInTransactionCallsRollbackOnException() throws Exception {
        when(callback.apply(transaction)).thenThrow(new Exception("callback error"));
        when(transaction.isComplete()).thenReturn(false);

        try {
            transactionManager.doInTransaction(callback);
            Assert.fail("expected exception");
        } catch (Throwable ex) {
            assertThat(ex.getMessage()).isEqualTo("callback error");
        }

        verify(transaction, never()).commit();
        verify(transaction).rollback();
        verify(callback).apply(transaction);
        assertThat(TransactionManager.getTransaction()).isNull();
    }

    @Test
    public void doInTransactionIgnoresErrorFromRollback() throws Exception {
        when(callback.apply(transaction)).thenThrow(new Exception("callback error"));
        when(transaction.isComplete()).thenReturn(false);
        when(transaction.rollback()).thenThrow(new IOException("rollback error"));

        try {
            transactionManager.doInTransaction(callback);
            Assert.fail("expected exception");
        } catch (Throwable ex) {
            assertThat(ex.getMessage()).isEqualTo("callback error");
        }

        verify(transaction, never()).commit();
        verify(transaction).rollback();
        verify(callback).apply(transaction);
        assertThat(TransactionManager.getTransaction()).isNull();
    }

    @Test
    public void doInTransactionDoesNotCallRollbackIfComplete() throws Exception {
        when(callback.apply(transaction)).thenThrow(new Exception("callback error"));
        when(transaction.isComplete()).thenReturn(true);

        try {
            transactionManager.doInTransaction(callback);
            Assert.fail("expected exception");
        } catch (Throwable ex) {
            assertThat(ex.getMessage()).isEqualTo("callback error");
        }

        verify(transaction, never()).commit();
        verify(transaction, never()).rollback();
        verify(callback).apply(transaction);
        assertThat(TransactionManager.getTransaction()).isNull();
    }

    @Test
    public void doInTransactionUsesExistingTransaction() throws Exception {
        long result = new Random().nextLong();
        when(callback.apply(transaction)).thenAnswer(invocation -> {
            assertThat(invocation.getArguments()[0]).isSameAs(transaction);
            assertThat(TransactionManager.getTransaction()).isSameAs(transaction);
            return transactionManager.doInTransaction(tx -> {
                assertThat(tx).isSameAs(transaction);
                return result;
            });
        });
        when(transaction.isComplete()).thenReturn(false);

        assertThat(transactionManager.doInTransaction(callback)).isEqualTo(result);

        verify(transaction).commit();
        verify(transaction, never()).rollback();
        verify(callback).apply(transaction);
        assertThat(TransactionManager.getTransaction()).isNull();
    }
}