package io.kestra.plugin.surrealdb;

import io.kestra.core.models.tasks.Task;

public class SurrealDBTest extends Task implements SurrealDBConnectionInterface {
	protected static final String USER = "Administrator";
	protected static final String PASSWORD = "password";
	protected static final String TABLE = "testtable_";
	protected static final String NAMESPACE = "some-namespace";
	protected static final String DATABASE = "some-database";
	protected static final String HOST = "127.0.0.1";
	protected static final int PORT = 8000;

	protected Query.QueryBuilder authentifiedQueryBuilder() {
		return Query.builder()
				.host(HOST)
				.port(PORT)
				.namespace(NAMESPACE)
				.database(DATABASE)
				.username(USER)
				.password(PASSWORD);
	}

}
