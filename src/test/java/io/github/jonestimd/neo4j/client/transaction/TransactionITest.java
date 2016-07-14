package io.github.jonestimd.neo4j.client.transaction;

import java.util.Arrays;
import java.util.Collections;
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

import static org.fest.assertions.Assertions.*;

public class TransactionITest {
    private final HttpDriver httpDriver = new ApacheHttpDriver("neo4j", "neo4jx", "localhost", 7474);

    @Test
    public void commit() throws Exception {
        Transaction transaction = new Transaction(httpDriver, "http://localhost:7474/db/data/transaction");
        Response response = transaction.commit(
            new Statement("merge (n:Item {itemId: {id}}) return n", Collections.singletonMap("id", 1L)),
            new Statement("merge (n:Item {itemId: {id}}) return n", Collections.singletonMap("id", 2L)),
            new Statement("match (n:Item {itemId: {ids}[0]}), (x:Item {itemId: {ids}[1]}) with n, x merge (n)<-[:RELATED_TO]-(x)", Collections.singletonMap("ids", Arrays.asList(1L, 2L))),
            new Statement("match p=(n:Item {itemId: {id}})<--(x) return n, p", Collections.singletonMap("id", 1L)));

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
        Transaction transaction = new Transaction(httpDriver, "http://localhost:7474/db/data/transaction");
        Response response = transaction.execute(new Statement("match p=(n:Item {itemId: {id}})--(x) return n, p", Collections.singletonMap("id", 2L)));

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
}