package com.findinpath.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.token;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@Testcontainers
public class SelectDistinctPrimaryKeysTest {

	private static final String DEMO_KEYSPACE_NAME = "DEMO";
	private static final String USER_BOOKMARKS_TABLE_NAME = "USER_BOOKMARKS";
	private static final String USER_ID_COLUMN_NAME = "user_id";
	private static final String TIMESTAMP_COLUMN_NAME = "timestamp";
	private static final String URL_COLUMN_NAME = "url";
	private static final String CREATE_DEMO_KEYSPACE_DDL =
			"CREATE KEYSPACE DEMO \n" +
					"WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }";
	private static final String CREATE_USER_BOOKMARKS_TABLE_DDL =
			"CREATE TABLE DEMO.USER_BOOKMARKS (\n" +
					"user_id UUID,\n" +
					"timestamp TIMEUUID,\n" +
					"url VARCHAR,\n" +
					"PRIMARY KEY (user_id, timestamp)\n" +
					") WITH CLUSTERING ORDER BY (timestamp DESC)\n";
	private static String[] URLS = new String[]{
			"https://www.github.com",
			"https://www.google.com",
			"https://www.twitter.com",
			"https://www.yahoo.com",
			"https://www.topcoder.com",
			"https://www.twitter.com",
			"https://delicio.us",
			"https://www.wikipedia.com",
			"https://www.nasa.gov"
	};

	@Test
	public void demo() {
		try (var cassandraContainer = new CassandraContainer<>()) {
			cassandraContainer.start();
			// Given a number of usersCount users have corresponding bookmark entries
			// in the demo.user_bookmarks table
			var usersCount = 10_000;
			setupUserBookmarks(cassandraContainer, usersCount);
			var pageSize = 1_000;
			// When selecting the distinct number of users from the table demo.user_bookmarks
			var readUsersCount = countDistinctUsers(cassandraContainer, pageSize);
			// Then the readUsersCount should be the same as the usersCount
			assertThat(readUsersCount, equalTo(usersCount));
		}
	}

	private int countDistinctUsers(CassandraContainer cassandraContainer, int pageSize) {
		try (var session = cassandraContainer.getCluster().connect()) {

			var lastUserIdProcessed = Optional.<UUID>empty();
			var countDistinctUsers = 0;
			do {
				var userIdsResultSet = getDistinctUserIdsBatch(session, lastUserIdProcessed, pageSize);
				var userIdsRows = userIdsResultSet.all();
				if (userIdsRows.isEmpty()) {
					break;
				}
				lastUserIdProcessed = Optional.of(userIdsRows.get(userIdsRows.size() - 1).getUUID(0));
				countDistinctUsers += userIdsRows.size();
			} while (true);

			return countDistinctUsers;
		}
	}

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

	private void setupUserBookmarks(CassandraContainer cassandraContainer, int usersCount) {
		try (var session = cassandraContainer.getCluster().connect()) {
			session.execute(CREATE_DEMO_KEYSPACE_DDL);
			session.execute(CREATE_USER_BOOKMARKS_TABLE_DDL);

			IntStream.rangeClosed(1, usersCount).forEach(userIndex -> insertUserBookmarks(session));
		}
	}

	private void insertUserBookmarks(Session session) {
		var userId = UUID.randomUUID();
		IntStream.rangeClosed(0, new Random().nextInt(5)).forEach(
				eventIndex -> {
					var timestamp = UUIDs.startOf(
							Instant.now().minus(eventIndex, ChronoUnit.DAYS).toEpochMilli()
					);
					var url = URLS[new Random().nextInt(URLS.length)];
					var insertBookmark = QueryBuilder
							.insertInto(DEMO_KEYSPACE_NAME, USER_BOOKMARKS_TABLE_NAME)
							.value(USER_ID_COLUMN_NAME, userId)
							.value(TIMESTAMP_COLUMN_NAME, timestamp)
							.value(URL_COLUMN_NAME, url);

					session.execute(insertBookmark);
				}
		);
	}
}
