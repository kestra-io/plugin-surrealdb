package io.kestra.plugin.surrealdb;

import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class QueryTest extends SurrealDBTest {

	@Inject
	private RunContextFactory runContextFactory;

	@Inject
	private StorageInterface storageInterface;

	@Test
	void simpleQuery_AllTypesParsed() throws Exception {
		RunContext runContext = runContextFactory.of();

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		authentifiedQueryBuilder()
				.query("""
      CREATE %s:%s 
      SET c_string = 'Kestra Doc', c_null = NULL, c_boolean = true, 
      c_int = 3, c_decimal = 3.10, c_decimal_e_notation = 3000, 
      c_number_array = [3, 3.10, 3000], c_string_array = ['firstString', 'secondString'],
			c_object = [
			{
			    c_object_prop:'hello',
			    c_subobject: [
			    {
			            c_subobject_prop:5
		      }
			        ]
      }],
      c_date = '2006-01-02T15:04:05.567+08:00'
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		Query query = authentifiedQueryBuilder()
				.query("SELECT * FROM %s:%s".formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build();

		Query.Output queryResult = query.run(runContext);

		authentifiedQueryBuilder()
				.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		assertThat(queryResult.getSize(), is(1L));

		Map<Object, Object> row = toMap(queryResult);;
		assertThat(row.get("c_string"), is("Kestra Doc"));
		assertThat(row.get("c_null"), nullValue());
		assertThat(row.get("c_boolean"), is(true));
		assertThat(row.get("c_int"), is(3.0));
		assertThat(row.get("c_decimal"), is(3.10));
		assertThat(row.get("c_decimal_e_notation"), is(3000.0));
		assertThat((Iterable<Number>) row.get("c_number_array"), Matchers.hasItems(3.0, 3.10, 3000.0));
		assertThat((Iterable<String>) row.get("c_string_array"), Matchers.hasItems("firstString", "secondString"));

		Map<Object, Object> object = toMap((List<Object>) row.get("c_object"));
		assertThat(object, aMapWithSize(2));
		assertThat(object.get("c_object_prop"), is("hello"));
		Map<Object, Object> subObject = toMap((List<Object>) object.get("c_subobject"));
		assertThat(subObject, aMapWithSize(1));
		assertThat(subObject, hasEntry("c_subobject_prop", 5.0));

		assertThat(row.get("c_date"), is("2006-01-02T07:04:05.567Z"));
	}

	@Test
	void simpleQuery_WorksWithId() throws Exception {
		RunContext runContext = runContextFactory.of();

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		authentifiedQueryBuilder()
				.query("CREATE %s:%s SET c_string='A collection doc'".formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		Query query = authentifiedQueryBuilder()
				.query("SELECT * FROM %s:%s WHERE c_string='A collection doc'".formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build();

		Query.Output queryResult = query.run(runContext);

		authentifiedQueryBuilder()
				.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		assertThat(queryResult.getSize(), is(1L));

		Map<Object, Object> row = toMap(queryResult);
		assertThat(row.get("c_string"), is("A collection doc"));
	}

	@ParameterizedTest
	@CsvSource(value = {
			"Kestra Doc;3;{\"c_string\":\"Kestra Doc\",\"c_int\":3}",
			"Kestra Doc;3;{\"c_int\":3,\"c_string\":\"Kestra Doc\"}"
	}, delimiter = ';')
	void preparedStatement(String firstArg, String secondArg, String parametersJson) throws Exception {
		RunContext runContext = runContextFactory.of();

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		authentifiedQueryBuilder()
				.query("""
      CREATE %s:%s 
      SET c_string = 'Kestra Doc', c_int = 3
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		Query query = authentifiedQueryBuilder()
				.query("""
				       SELECT c_string, c_int
				       FROM %s
				       WHERE c_string = '%s' AND c_int = %s
				       """.formatted(TABLE, firstArg, secondArg))
				.fetchType(FetchType.FETCH_ONE)
				.parameters(JacksonMapper.toMap(parametersJson))
				.build();

		Query.Output queryResult = query.run(runContext);

		authentifiedQueryBuilder()
				.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		assertThat(queryResult.getSize(), is(1L));

		Map<Object, Object> row = toMap(queryResult);
		assertThat(row.get("c_string"), is("Kestra Doc"));
		assertThat(row.get("c_int"), is(3.0));
	}

	@Test
	void simpleQuery_FetchAll() throws Exception {
		RunContext runContext = runContextFactory.of();

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		authentifiedQueryBuilder()
				.query("""
      CREATE %s:%s 
      SET c_string = 'Kestra Doc', c_int = 3
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		Query.Output insertQuery = authentifiedQueryBuilder()
				.query("""
			      UPDATE %s SET c_string = 'Another Kestra Doc' RETURN *""".formatted(TABLE))
				.fetchType(FetchType.NONE)
				.build().run(runContext);

		// Only available if adding 'RETURNING *' to insert
		assertThat(insertQuery.getSize(), is(1L));

		Query query = authentifiedQueryBuilder()
				.query("SELECT * FROM " + TABLE)
				.fetchType(FetchType.FETCH)
				.build();

		Query.Output queryResult = query.run(runContext);

		authentifiedQueryBuilder()
				.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		assertThat(queryResult.getSize(), is(1L));

		List<Map<Object, Object>> rows = queryResult.getRows().stream().map(row -> toMap(row.getResult())).toList();
		assertThat(rows, hasSize(2));
		assertThat(rows, Matchers.hasItems(
				hasEntry("c_string", "Kestra Doc"),
				hasEntry("c_string", "Another Kestra Doc")));

		// If we precise field, we get rid of bucket layer in output
		query = authentifiedQueryBuilder()
				.query("SELECT c_string FROM " + TABLE)
				.fetchType(FetchType.FETCH)
				.build();

		queryResult = query.run(runContext);

		assertThat(queryResult.getSize(), is(2L));

		rows = queryResult.getRows().stream().map(row -> toMap(row.getResult())).toList();
		assertThat(rows, hasSize(2));
		assertThat(rows, Matchers.hasItems(
				hasEntry("c_string", "Kestra Doc"),
				hasEntry("c_string", "Another Kestra Doc")
		                                  ));
	}

	@Test
	void simpleQuery_ToInternalStorage() throws Exception {
		RunContext runContext = runContextFactory.of();

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");

		authentifiedQueryBuilder()
				.query("CREATE %s:%s SET c_string='A collection doc'".formatted(TABLE, id))
				.fetchType(FetchType.STORE)
				.build();

		Query query = authentifiedQueryBuilder()
				.query("SELECT * FROM %s WHERE c_string='A collection doc'".formatted(TABLE))
				.fetchType(FetchType.STORE)
				.build();

		Query.Output queryResult = query.run(runContext);

		authentifiedQueryBuilder()
				.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
				.fetchType(FetchType.FETCH_ONE)
				.build().run(runContext);

		assertThat(queryResult.getSize(), is(1L));

		String outputFileContent = IOUtils.toString(storageInterface.get(queryResult.getUri()), Charsets.UTF_8);
		Map[] rows = JacksonMapper.ofIon().readValue(outputFileContent, Map[].class);

		assertThat(rows.length, is(1));
		assertThat(((Map) rows[0].get(COLLECTION)).get("c_string"), is("A collection doc"));
	}

	private static Map<Object, Object> toMap(Query.Output queryResult) {
		return toMap(queryResult.getRow().getResult());
	}

	private static Map<Object, Object> toMap(List<Object> list) {
		return list.stream()
				.flatMap(entry -> ((Map<String, String>) entry).entrySet().stream())
				.collect(HashMap::new, (hashMap, entry) -> hashMap.put(entry.getKey(), entry.getValue()), HashMap::putAll);
	}
}
