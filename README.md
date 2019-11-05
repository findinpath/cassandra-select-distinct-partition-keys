Demo on how to select the distinct partition keys of a table
============================================================


This is a simple showcase on how to select all the distinct partition keys
from a Cassandra table.


The way to select a page of distinct partition keys is summarized in the following code snippet:

```java
private ResultSet getDistinctUserIdsBatch(Session session, Optional<UUID> lastUserIdProcessed,
        int batchSize) {
    var batchedDistinctUserSelect = QueryBuilder.select(USER_ID_COLUMN_NAME)
            .distinct()
            .from(DEMO_KEYSPACE_NAME, USER_BOOKMARKS_TABLE_NAME);

    lastUserIdProcessed.ifPresent(
            uuid -> batchedDistinctUserSelect.where(gt(token(USER_ID_COLUMN_NAME), token(uuid))));

    batchedDistinctUserSelect.limit(batchSize);

    return session.execute(batchedDistinctUserSelect);
}
``` 


The project contains a unit test which spawns a Cassandra 
container (via [testcontainers](https://www.testcontainers.org/) library)
, fills it with data (user bookmarks) and then selects all the distinct user ids
from the table in a page fashion. 