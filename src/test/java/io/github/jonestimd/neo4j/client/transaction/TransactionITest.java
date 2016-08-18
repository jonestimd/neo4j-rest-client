package io.github.jonestimd.neo4j.client.transaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import io.github.jonestimd.neo4j.client.http.ApacheHttpDriver;
import io.github.jonestimd.neo4j.client.http.HttpDriver;
import io.github.jonestimd.neo4j.client.transaction.request.Statement;
import io.github.jonestimd.neo4j.client.transaction.response.ColumnMeta.MetaType;
import io.github.jonestimd.neo4j.client.transaction.response.Node;
import io.github.jonestimd.neo4j.client.transaction.response.Relationship;
import io.github.jonestimd.neo4j.client.transaction.response.Response;
import io.github.jonestimd.neo4j.client.transaction.response.StatementResult;
import org.junit.Test;

import static java.util.Collections.*;
import static org.fest.assertions.Assertions.*;

public class TransactionITest {
    public static final String BASE_URL = "http://localhost:7474/db/data/transaction";
    private final HttpDriver httpDriver = new ApacheHttpDriver("neo4j", "neo4jx", "localhost", 7474);
    private final TransactionManager transactionManager = new TransactionManager(httpDriver, BASE_URL, null, null, 0L);

    @Test
    public void doInTransaction() throws Exception {
        Response response = transactionManager.doInTransaction(tx -> tx.commit(
                new Statement("merge (n:Item {itemId: {id}}) return n", singletonMap("id", 1L)),
                new Statement("merge (n:Item {itemId: {id}}) return n", singletonMap("id", 2L)),
                new Statement("match (n:Item {itemId: {ids}[0]}), (x:Item {itemId: {ids}[1]}) with n, x merge (n)<-[:RELATED_TO]-(x)", singletonMap("ids", Arrays.asList(1L, 2L))),
                new Statement("match p=(n:Item {itemId: {id}})<--(x) return n, p", singletonMap("id", 1L))));

        assertThat(response.next()).isTrue();
        assertThat(response.next()).isTrue();
        assertThat(response.next()).isTrue();
        assertThat(response.next()).isTrue();
        StatementResult result = response.getResult();
        assertThat(result.next()).isTrue();
        assertThat(result.getColumn("n").getProperties().keySet()).containsOnly("itemId");
        assertThat(result.getMeta("n")).hasSize(1);
        assertThat(result.getMeta("n").get(0).getType()).isEqualTo(MetaType.NODE);
        assertThat(result.getColumn("p").getList()).hasSize(3);
        assertThat(result.getMeta("p")).hasSize(3);
        assertThat(result.getMeta("p").get(0).getType()).isEqualTo(MetaType.NODE);
        assertThat(result.getMeta("p").get(1).getType()).isEqualTo(MetaType.RELATIONSHIP);
        assertThat(result.getMeta("p").get(2).getType()).isEqualTo(MetaType.NODE);
        List<Node> nodes = result.getNodes();
        assertThat(nodes).hasSize(2);
        List<Relationship> relationships = result.getRelationships();
        assertThat(relationships).hasSize(1);
        assertThat(result.next()).isFalse();
        assertThat(response.next()).isFalse();
    }

    @Test
    public void executeAndCommit() throws Exception {
        Transaction transaction = new Transaction(httpDriver, BASE_URL);
        Response response = transaction.execute(new Statement("match p=(n:Item {itemId: {id}})--(x) return n, p", singletonMap("id", 2L)));

        assertThat(response.next()).isTrue();
        StatementResult result = response.getResult();
        assertThat(result.next()).isTrue();
        assertThat(result.getColumn("n").getProperties().keySet()).contains("itemId");
        assertThat(result.getMeta("n")).hasSize(1);
        assertThat(result.getMeta("n").get(0).getType()).isEqualTo(MetaType.NODE);
        assertThat(result.getColumn("p").getList()).hasSize(3);
        assertThat(result.getMeta("p")).hasSize(3);
        assertThat(result.getMeta("p").get(0).getType()).isEqualTo(MetaType.NODE);
        assertThat(result.getMeta("p").get(1).getType()).isEqualTo(MetaType.RELATIONSHIP);
        assertThat(result.getMeta("p").get(2).getType()).isEqualTo(MetaType.NODE);
        List<Node> nodes = result.getNodes();
        assertThat(nodes).hasSize(2);
        List<Relationship> relationships = result.getRelationships();
        assertThat(relationships).hasSize(1);
        assertThat(result.next()).isFalse();
        assertThat(response.next()).isFalse();

        Response commit = transaction.commit();

        assertThat(commit.next()).isFalse();
    }

    @Test
    public void rollback() throws Exception {
        transactionManager.runInTransaction(tx -> tx.commit(new Statement("match (n:Item {itemId: -999}) detach delete n", emptyMap())));
        Transaction transaction = new Transaction(httpDriver, BASE_URL);
        Response response1 = transaction.execute(new Statement("merge (n:Item {itemId: -999}) return n", emptyMap()));
        response1.next();
        response1.getResult().next();
        Long graphId = response1.getResult().getMeta("n").get(0).getId();

        transaction.rollback();

        StatementResult result = transactionManager.doInTransaction(tx -> findById(tx, graphId));
        assertThat(result.next()).isFalse();
    }

    private StatementResult findById(Transaction transaction, Long graphId) throws IOException {
        Response response = transaction.commit(new Statement("match (n) where id(n) = {id} return n", singletonMap("id", graphId)));
        response.next();
        return response.getResult();
    }
}