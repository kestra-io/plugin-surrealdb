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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

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

		Map<String, String> parameters = new HashMap<>();
		parameters.put("name", "Kestra Doc");
		parameters.put("nullable", null);
		parameters.put("bool", String.valueOf(true));
		parameters.put("int", String.valueOf(3));
		parameters.put("dec", String.valueOf(3.10));
		parameters.put("bigdec", String.valueOf(3000));
		parameters.put("date", String.valueOf(LocalDateTime.of(2006, 1, 2, 15, 4, 5, 5_670_000_00).atOffset(ZoneOffset.of("+8"))));

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		Query.Output queryCreation = authentifiedQueryBuilder()
			.query("""
      CREATE %s:%s 
      SET c_string = $name, c_null = $nullable, c_boolean = <bool> $bool, 
      c_int = <int> $int, c_decimal = <number> $dec, c_decimal_e_notation = <number> $bigdec, 
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
      c_date = <datetime> $date
      """.formatted(TABLE, id))
			.fetchType(FetchType.FETCH_ONE)
			.parameters(parameters)
			.build().run(runContext);

		Map<String, Object> creationRows = queryCreation.getRow();
		assertThat(creationRows.get("c_string"), is("Kestra Doc"));
		assertThat(creationRows.get("c_null"), nullValue());
		assertThat(creationRows.get("c_boolean"), is(true));
		assertThat(creationRows.get("c_int"), is(3.0));
		assertThat(creationRows.get("c_decimal"), is(3.10));
		assertThat(creationRows.get("c_decimal_e_notation"), is(3000.0));
		assertThat((Iterable<Number>) creationRows.get("c_number_array"), Matchers.hasItems(3.0, 3.10, 3000.0));
		assertThat((Iterable<String>) creationRows.get("c_string_array"), Matchers.hasItems("firstString", "secondString"));

		Map<Object, Object> object = toMap((List<Object>) creationRows.get("c_object"));
		assertThat(object, aMapWithSize(2));
		assertThat(object.get("c_object_prop"), is("hello"));
		Map<Object, Object> subObject = toMap((List<Object>) object.get("c_subobject"));
		assertThat(subObject, aMapWithSize(1));
		assertThat(subObject, hasEntry("c_subobject_prop", 5.0));

		assertThat(creationRows.get("c_date"), is("2006-01-02T07:04:05.567Z"));

		Query query = authentifiedQueryBuilder()
			.query("SELECT * FROM %s:%s".formatted(TABLE, id))
			.fetchType(FetchType.FETCH_ONE)
			.build();

		Query.Output queryResult = query.run(runContext);

		Query.Output queryDelete = authentifiedQueryBuilder()
			.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryDelete.getRow(), nullValue());
		assertThat(queryDelete.getRows(), nullValue());

		assertThat(queryResult.getSize(), is(1L));

		Map<String, Object> row = queryResult.getRow();
		assertThat(row.get("c_string"), is("Kestra Doc"));
		assertThat(row.get("c_null"), nullValue());
		assertThat(row.get("c_boolean"), is(true));
		assertThat(row.get("c_int"), is(3.0));
		assertThat(row.get("c_decimal"), is(3.10));
		assertThat(row.get("c_decimal_e_notation"), is(3000.0));
		assertThat((Iterable<Number>) row.get("c_number_array"), Matchers.hasItems(3.0, 3.10, 3000.0));
		assertThat((Iterable<String>) row.get("c_string_array"), Matchers.hasItems("firstString", "secondString"));

		object = toMap((List<Object>) row.get("c_object"));
		assertThat(object, aMapWithSize(2));
		assertThat(object.get("c_object_prop"), is("hello"));
		subObject = toMap((List<Object>) object.get("c_subobject"));
		assertThat(subObject, aMapWithSize(1));
		assertThat(subObject, hasEntry("c_subobject_prop", 5.0));

		assertThat(row.get("c_date"), is("2006-01-02T07:04:05.567Z"));
	}

	@Test
	void simpleQuery_WorksWithId() throws Exception {
		RunContext runContext = runContextFactory.of();

		Map<String, String> parameters = Map.of("name", "A collection doc");

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");

		Query.Output queryCreate = authentifiedQueryBuilder()
			.query("CREATE %s:%s SET c_string=$name".formatted(TABLE, id))
			.parameters(parameters)
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryCreate.getSize(), is(1L));
		assertThat(queryCreate.getRow().get("c_string"), is("A collection doc"));

		Query query = authentifiedQueryBuilder()
			.query("SELECT * FROM %s:%s WHERE c_string=$name".formatted(TABLE, id))
			.parameters(parameters)
			.fetchType(FetchType.FETCH_ONE)
			.build();

		Query.Output queryResult = query.run(runContext);

		Query.Output queryDelete = authentifiedQueryBuilder()
			.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryDelete.getRow(), nullValue());
		assertThat(queryDelete.getRows(), nullValue());

		assertThat(queryResult.getSize(), is(1L));
		assertThat(queryResult.getRow().get("c_string"), is("A collection doc"));
	}

	@ParameterizedTest
	@CsvSource(value = {
		"$name;$age;{\"name\":\"Kestra Doc\",\"age\":3}",
		"$name;$age;{\"age\":3,\"name\":\"Kestra Doc\"}"
	}, delimiter = ';')
	void preparedStatement(String firstArg, String secondArg, String parametersJson) throws Exception {
		RunContext runContext = runContextFactory.of();

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		Map<String, Object> parameters = JacksonMapper.toMap(parametersJson);

		Query.Output queryCreate = authentifiedQueryBuilder()
			.query("""
      CREATE %s:%s 
      SET c_string = %s, c_int = <int> %s
      """.formatted(TABLE, id, firstArg, secondArg))
			.parameters(parameters)
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryCreate.getSize(), is(1L));

		Map<String, Object> createRow = queryCreate.getRow();
		assertThat(createRow.get("c_string"), is("Kestra Doc"));
		assertThat(createRow.get("c_int"), is(3.0));

		Query query = authentifiedQueryBuilder()
			.query("""
				       SELECT c_string, c_int
				       FROM %s
				       WHERE c_string = %s AND c_int = <int> %s
				       """.formatted(TABLE, firstArg, secondArg))
			.fetchType(FetchType.FETCH_ONE)
			.parameters(parameters)
			.build();

		Query.Output queryResult = query.run(runContext);

		Query.Output queryDelete = authentifiedQueryBuilder()
			.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryDelete.getRow(), nullValue());
		assertThat(queryDelete.getRows(), nullValue());

		assertThat(queryResult.getSize(), is(1L));

		Map<String, Object> row = queryResult.getRow();
		assertThat(row.get("c_string"), is("Kestra Doc"));
		assertThat(row.get("c_int"), is(3.0));
	}

	@Test
	void simpleQuery_FetchAll() throws Exception {
		RunContext runContext = runContextFactory.of();

		Map<String, String> parameters = Map.of("name", "Kestra Doc");

		String firstId = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		String secondId = UUID.randomUUID().toString().toLowerCase().replace("-", "");

		authentifiedQueryBuilder()
			.query("CREATE %s:%s SET c_string = $name".formatted(TABLE, firstId))
			.parameters(parameters)
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		authentifiedQueryBuilder()
			.query("CREATE %s:%s SET c_string = $name".formatted(TABLE, secondId))
			.parameters(parameters)
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		Query.Output updateQuery = authentifiedQueryBuilder()
			.query("""
			      UPDATE %s:%s SET c_string = $name RETURN *""".formatted(TABLE, secondId))
			.parameters(Map.of("name", "Another Kestra Doc"))
			.fetchType(FetchType.NONE)
			.build().run(runContext);

		// Only available if adding 'RETURNING *' to insert
		assertThat(updateQuery.getSize(), is(1L));

		Query query = authentifiedQueryBuilder()
			.query("SELECT * FROM " + TABLE)
			.fetchType(FetchType.FETCH)
			.build();

		Query.Output queryResult = query.run(runContext);

		assertThat(queryResult.getSize(), is(2L));

		List<Map<String, Object>> rows = queryResult.getRows();

		List<Map<String, Object>> objects = queryResult.getRows();

		assertThat(objects, hasSize(2));
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

		rows = queryResult.getRows();

		assertThat(rows, hasSize(2));
		assertThat(rows, Matchers.hasItems(
			hasEntry("c_string", "Kestra Doc"),
			hasEntry("c_string", "Another Kestra Doc")
		                                  ));

		Query.Output queryDelete = authentifiedQueryBuilder()
			.query("""
      DELETE %s:%s
      """.formatted(TABLE, firstId))
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryDelete.getRow(), nullValue());
		assertThat(queryDelete.getRows(), nullValue());

		queryDelete = authentifiedQueryBuilder()
			.query("""
      DELETE %s:%s
      """.formatted(TABLE, secondId))
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryDelete.getRow(), nullValue());
		assertThat(queryDelete.getRows(), nullValue());
	}

	@Test
	void simpleQuery_ToInternalStorage() throws Exception {
		RunContext runContext = runContextFactory.of();

		Map<String, String> parameters = Map.of("name", "A collection doc");

		String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
		Query.Output queryCreate = authentifiedQueryBuilder()
			.query("CREATE %s:%s SET c_string=$name".formatted(TABLE, id))
			.parameters(parameters)
			.fetchType(FetchType.STORE)
			.build().run(runContext);


		assertThat(queryCreate.getSize(), is(1L));

		String outputFileContent = IOUtils.toString(storageInterface.get(queryCreate.getUri()), Charsets.UTF_8);
		Map rows = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);

		assertThat(rows.get("c_string"), is("A collection doc"));

		Query query = authentifiedQueryBuilder()
			.query("SELECT * FROM %s WHERE c_string=$name".formatted(TABLE))
			.parameters(parameters)
			.fetchType(FetchType.STORE)
			.build();

		Query.Output queryResult = query.run(runContext);

		assertThat(queryResult.getSize(), is(1L));

		outputFileContent = IOUtils.toString(storageInterface.get(queryResult.getUri()), Charsets.UTF_8);
		rows = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);

		assertThat(rows.get("c_string"), is("A collection doc"));

		Query.Output queryDelete = authentifiedQueryBuilder()
			.query("""
      DELETE %s:%s
      """.formatted(TABLE, id))
			.fetchType(FetchType.FETCH_ONE)
			.build().run(runContext);

		assertThat(queryDelete.getRow(), nullValue());
		assertThat(queryDelete.getRows(), nullValue());
	}

	private static Map<Object, Object> toMap(List<Object> list) {
		return list.stream()
			.flatMap(entry -> ((Map<String, String>) entry).entrySet().stream())
			.collect(TreeMap::new, (hashMap, entry) -> hashMap.put(entry.getKey(), entry.getValue()), TreeMap::putAll);
	}
}
