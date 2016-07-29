# neo4j-rest-java
Java REST client for Neo4j database

### Features
- Executes Cypher queries using Neo4j's transaction API
- Combines multiple queries in a single HTTP request
- Automatically Pings transaction URL during long running task
 - Prevents transaction timeout
 - Configurable ping frequency
