# neo4j-rest-java
[![travis-ci.org](https://travis-ci.org/jonestimd/neo4j-rest-java.svg?branch=master)](https://travis-ci.org/jonestimd/neo4j-rest-java?branch=master)
Java REST client for Neo4j database

### Features
- Executes Cypher queries using Neo4j's transaction API
  - Combines multiple queries in a single HTTP request
  - Streams results for reduced memory requirement
- Automatically Pings transaction URL during long running task
  - Prevents transaction timeout
  - Configurable ping frequency

### Basic Usage
```Java
import java.util.Timer;
import io.github.jonestimd.neo4j.client.http.ApacheHttpDriver;
import io.github.jonestimd.neo4j.client.http.HttpDriver;
import io.github.jonestimd.neo4j.client.transaction.TransactionManager;
import io.github.jonestimd.neo4j.client.transaction.request.Statement;
import io.github.jonestimd.neo4j.client.transaction.response.Node;
import io.github.jonestimd.neo4j.client.transaction.response.Relationship;
import io.github.jonestimd.neo4j.client.transaction.response.Response;
import io.github.jonestimd.neo4j.client.transaction.response.StatementResult;

import static java.util.Collections.*;
import static io.github.jonestimd.neo4j.client.transaction.Transaction.*;

// configure the transaction manager
String baseUrl = "http://localhost:7474/db/data/transaction";
HttpDriver httpDriver = new ApacheHttpDriver("neo4j", "neo4j", "localhost", 7474);
Timer pingTimer = new Timer("ping timer", true);
TransactionManager transactionManager = new TransactionManager(httpDriver, baseUrl,
       DEFAULT_JSON_FACTORY, pingTimer, 45000L);

// run a query
Response response = transactionManager.doInTransaction(tx -> tx.execute(
        new Statement("match p=(n:Item {itemId: {id}})<--(x) return n, p", singletonMap("id", 1L))));

// process the results
while (response.next()) {
    StatementResult result = response.getResult();
    while (result.next()) {
        result.getColumn("n");
        result.getColumn("p");
        result.getMeta("n");
        result.getMeta("p");
        result.getNodes();
        result.getRelationships();
    }
}
```
