package io.kestra.plugin.surrealdb;

public class SurrealDBTest {
	protected static final String TABLE = "testtable_";
	protected static final String NAMESPACE = "some-namespace";
	protected static final String DATABASE = "some-database";
	protected static final String HOST = "127.0.0.1";

	protected Query.QueryBuilder authentifiedQueryBuilder() {
		return Query.builder()
			.host(HOST)
			.namespace(NAMESPACE)
			.database(DATABASE);
	}

}
