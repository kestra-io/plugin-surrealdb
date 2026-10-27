package io.kestra.plugin.surrealdb;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.surrealdb.connection.exception.SurrealException;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class QueryTest extends SurrealDBTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @ParameterizedTest
    @ValueSource(strings = { "", "port: 8000", "port: \"{{ inputs.port }}\"" })
    void queryWithPortFromYaml(String port) throws Exception {
        var runContext = runContextFactory.of(Map.of("inputs", Map.of("port", "8000")));
        var query = YamlParser.parse(
            """
                id: %s
                type: io.kestra.plugin.surrealdb.Query
                host: 127.0.0.1
                %s
                username: Administrator
                password: password
                namespace: some-namespace
                database: some-database
                fetchType: FETCH_ONE
                query: "RETURN [{ source: 'port_test' }]"
                """.formatted(IdUtils.create(), port), Query.class
        );

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRow().get("source"), is("port_test"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "-1", "65536", "70000" })
    void queryRejectsPortOutsideRange(String port) throws Exception {
        var query = portFromInput();
        var runContext = TestsUtils.mockRunContext(runContextFactory, query, Map.of("port", port));

        assertThrows(ConstraintViolationException.class, () -> query.run(runContext));
    }

    @Test
    void queryRejectsPortThatIsNotANumber() throws Exception {
        var query = portFromInput();
        var runContext = TestsUtils.mockRunContext(runContextFactory, query, Map.of("port", "abc"));

        assertThrows(IllegalVariableEvaluationException.class, () -> query.run(runContext));
    }

    private Query portFromInput() {
        return YamlParser.parse(
            """
                id: %s
                type: io.kestra.plugin.surrealdb.Query
                host: 127.0.0.1
                port: "{{ inputs.port }}"
                username: Administrator
                password: password
                namespace: some-namespace
                database: some-database
                fetchType: FETCH_ONE
                query: "RETURN [{ source: 'port_test' }]"
                """.formatted(IdUtils.create()), Query.class
        );
    }

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
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .parameters(Property.ofValue(parameters))
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
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        Query.Output queryResult = query.run(runContext);

        Query.Output queryDelete = authentifiedQueryBuilder()
            .query("""
                DELETE %s:%s
                """.formatted(TABLE, id))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
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
            .parameters(Property.ofValue(parameters))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        assertThat(queryCreate.getSize(), is(1L));
        assertThat(queryCreate.getRow().get("c_string"), is("A collection doc"));

        Query query = authentifiedQueryBuilder()
            .query("SELECT * FROM %s:%s WHERE c_string=$name".formatted(TABLE, id))
            .parameters(Property.ofValue(parameters))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        Query.Output queryResult = query.run(runContext);

        Query.Output queryDelete = authentifiedQueryBuilder()
            .query("""
                DELETE %s:%s
                """.formatted(TABLE, id))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        assertThat(queryDelete.getRow(), nullValue());
        assertThat(queryDelete.getRows(), nullValue());

        assertThat(queryResult.getSize(), is(1L));
        assertThat(queryResult.getRow().get("c_string"), is("A collection doc"));
    }

    @ParameterizedTest
    @CsvSource(
        value = {
            "$name;$age;{\"name\":\"Kestra Doc\",\"age\":3}",
            "$name;$age;{\"age\":3,\"name\":\"Kestra Doc\"}"
        }, delimiter = ';'
    )
    void preparedStatement(String firstArg, String secondArg, String parametersJson) throws Exception {
        RunContext runContext = runContextFactory.of();

        String id = UUID.randomUUID().toString().toLowerCase().replace("-", "");
        Map<String, Object> parameters = JacksonMapper.toMap(parametersJson);

        Query.Output queryCreate = authentifiedQueryBuilder()
            .query("""
                CREATE %s:%s
                SET c_string = %s, c_int = <int> %s
                """.formatted(TABLE, id, firstArg, secondArg))
            .parameters(Property.ofValue(parameters))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
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
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .parameters(Property.ofValue(parameters))
            .build();

        Query.Output queryResult = query.run(runContext);

        Query.Output queryDelete = authentifiedQueryBuilder()
            .query("""
                DELETE %s:%s
                """.formatted(TABLE, id))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
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
            .parameters(Property.ofValue(parameters))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        authentifiedQueryBuilder()
            .query("CREATE %s:%s SET c_string = $name".formatted(TABLE, secondId))
            .parameters(Property.ofValue(parameters))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        Query.Output updateQuery = authentifiedQueryBuilder()
            .query("""
                UPDATE %s:%s SET c_string = $name RETURN *""".formatted(TABLE, secondId))
            .parameters(Property.ofValue(Map.of("name", "Another Kestra Doc")))
            .fetchType(Property.ofValue(FetchType.NONE))
            .build().run(runContext);

        // Only available if adding 'RETURNING *' to insert
        assertThat(updateQuery.getSize(), is(1L));

        Query query = authentifiedQueryBuilder()
            .query("SELECT * FROM " + TABLE)
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        Query.Output queryResult = query.run(runContext);

        assertThat(queryResult.getSize(), is(2L));

        List<Map<String, Object>> rows = queryResult.getRows();

        List<Map<String, Object>> objects = queryResult.getRows();

        assertThat(objects, hasSize(2));
        assertThat(
            rows, Matchers.hasItems(
                hasEntry("c_string", "Kestra Doc"),
                hasEntry("c_string", "Another Kestra Doc")
            )
        );

        // If we precise field, we get rid of bucket layer in output
        query = authentifiedQueryBuilder()
            .query("SELECT c_string FROM " + TABLE)
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        queryResult = query.run(runContext);

        assertThat(queryResult.getSize(), is(2L));

        rows = queryResult.getRows();

        assertThat(rows, hasSize(2));
        assertThat(
            rows, Matchers.hasItems(
                hasEntry("c_string", "Kestra Doc"),
                hasEntry("c_string", "Another Kestra Doc")
            )
        );

        Query.Output queryDelete = authentifiedQueryBuilder()
            .query("""
                DELETE %s:%s
                """.formatted(TABLE, firstId))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        assertThat(queryDelete.getRow(), nullValue());
        assertThat(queryDelete.getRows(), nullValue());

        queryDelete = authentifiedQueryBuilder()
            .query("""
                DELETE %s:%s
                """.formatted(TABLE, secondId))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
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
            .parameters(Property.ofValue(parameters))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build().run(runContext);

        assertThat(queryCreate.getSize(), is(1L));

        String outputFileContent = IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, queryCreate.getUri()), Charsets.UTF_8);
        Map rows = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);

        assertThat(rows.get("c_string"), is("A collection doc"));

        Query query = authentifiedQueryBuilder()
            .query("SELECT * FROM %s WHERE c_string=$name".formatted(TABLE))
            .parameters(Property.ofValue(parameters))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        Query.Output queryResult = query.run(runContext);

        assertThat(queryResult.getSize(), is(1L));

        outputFileContent = IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, queryResult.getUri()), Charsets.UTF_8);
        rows = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);

        assertThat(rows.get("c_string"), is("A collection doc"));

        Query.Output queryDelete = authentifiedQueryBuilder()
            .query("""
                DELETE %s:%s
                """.formatted(TABLE, id))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        assertThat(queryDelete.getRow(), nullValue());
        assertThat(queryDelete.getRows(), nullValue());
    }

    @Test
    void nonListQuery_ReturnScalar() throws Exception {
        RunContext runContext = runContextFactory.of();

        // FETCH
        Query queryFetch = authentifiedQueryBuilder()
            .query("RETURN 1")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        Query.Output outputFetch = queryFetch.run(runContext);
        assertThat(outputFetch.getSize(), is(1L));
        assertThat(outputFetch.getRows(), hasSize(1));
        assertThat(outputFetch.getRows().get(0).get("result"), is(1.0));

        // FETCH_ONE
        Query queryFetchOne = authentifiedQueryBuilder()
            .query("RETURN 1")
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();
        Query.Output outputFetchOne = queryFetchOne.run(runContext);
        assertThat(outputFetchOne.getSize(), is(1L));
        assertThat(outputFetchOne.getRow().get("result"), is(1.0));

        // STORE
        Query queryStore = authentifiedQueryBuilder()
            .query("RETURN 1")
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();
        Query.Output outputStore = queryStore.run(runContext);
        assertThat(outputStore.getSize(), is(1L));
        String outputFileContent = IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, outputStore.getUri()), Charsets.UTF_8);
        Map<?, ?> storedRow = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);
        assertThat(((Number) storedRow.get("result")).doubleValue(), is(1.0));
    }

    @Test
    void nonListQuery_ReturnObject() throws Exception {
        RunContext runContext = runContextFactory.of();

        // FETCH
        Query queryFetch = authentifiedQueryBuilder()
            .query("RETURN {a: 1}")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        Query.Output outputFetch = queryFetch.run(runContext);
        assertThat(outputFetch.getSize(), is(1L));
        assertThat(outputFetch.getRows(), hasSize(1));
        assertThat(outputFetch.getRows().get(0).get("a"), is(1.0));

        // FETCH_ONE
        Query queryFetchOne = authentifiedQueryBuilder()
            .query("RETURN {a: 1}")
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();
        Query.Output outputFetchOne = queryFetchOne.run(runContext);
        assertThat(outputFetchOne.getSize(), is(1L));
        assertThat(outputFetchOne.getRow().get("a"), is(1.0));

        // STORE
        Query queryStore = authentifiedQueryBuilder()
            .query("RETURN {a: 1}")
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();
        Query.Output outputStore = queryStore.run(runContext);
        assertThat(outputStore.getSize(), is(1L));
        String outputFileContent = IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, outputStore.getUri()), Charsets.UTF_8);
        Map<?, ?> storedRow = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);
        assertThat(((Number) storedRow.get("a")).doubleValue(), is(1.0));
    }

    @Test
    void nonListQuery_InfoForDb() throws Exception {
        RunContext runContext = runContextFactory.of();

        // FETCH
        Query queryFetch = authentifiedQueryBuilder()
            .query("INFO FOR DB")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        Query.Output outputFetch = queryFetch.run(runContext);
        assertThat(outputFetch.getSize(), is(1L));
        assertThat(outputFetch.getRows(), hasSize(1));
        assertThat(outputFetch.getRows().get(0), hasKey("tables"));

        // FETCH_ONE
        Query queryFetchOne = authentifiedQueryBuilder()
            .query("INFO FOR DB")
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();
        Query.Output outputFetchOne = queryFetchOne.run(runContext);
        assertThat(outputFetchOne.getSize(), is(1L));
        assertThat(outputFetchOne.getRow(), hasKey("tables"));

        // STORE
        Query queryStore = authentifiedQueryBuilder()
            .query("INFO FOR DB")
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();
        Query.Output outputStore = queryStore.run(runContext);
        assertThat(outputStore.getSize(), is(1L));
        String outputFileContent = IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, outputStore.getUri()), Charsets.UTF_8);
        Map<?, ?> storedRow = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);
        assertThat(storedRow, hasKey("tables"));
    }

    @Test
    void nonListQuery_InfoForTable() throws Exception {
        RunContext runContext = runContextFactory.of();
        String table = "table_info_" + UUID.randomUUID().toString().toLowerCase().replace("-", "");

        authentifiedQueryBuilder()
            .query("DEFINE TABLE %s SCHEMAFULL".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        // FETCH
        Query queryFetch = authentifiedQueryBuilder()
            .query("INFO FOR TABLE %s".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        Query.Output outputFetch = queryFetch.run(runContext);
        assertThat(outputFetch.getSize(), is(1L));
        assertThat(outputFetch.getRows(), hasSize(1));
        assertThat(outputFetch.getRows().get(0), hasKey("fields"));

        // FETCH_ONE
        Query queryFetchOne = authentifiedQueryBuilder()
            .query("INFO FOR TABLE %s".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();
        Query.Output outputFetchOne = queryFetchOne.run(runContext);
        assertThat(outputFetchOne.getSize(), is(1L));
        assertThat(outputFetchOne.getRow(), hasKey("fields"));

        // STORE
        Query queryStore = authentifiedQueryBuilder()
            .query("INFO FOR TABLE %s".formatted(table))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();
        Query.Output outputStore = queryStore.run(runContext);
        assertThat(outputStore.getSize(), is(1L));
        String outputFileContent = IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, outputStore.getUri()), Charsets.UTF_8);
        Map<?, ?> storedRow = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);
        assertThat(storedRow, hasKey("fields"));
    }

    @Test
    void multiStatement_MixSelectAndInfoFor() throws Exception {
        RunContext runContext = runContextFactory.of();
        String table = "table_mix_" + UUID.randomUUID().toString().toLowerCase().replace("-", "");

        authentifiedQueryBuilder()
            .query("CREATE %s:1 SET name='first'; CREATE %s:2 SET name='second';".formatted(table, table))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);

        Query query = authentifiedQueryBuilder()
            .query("SELECT * FROM %s; INFO FOR DB;".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        Query.Output output = query.run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows(), hasSize(3));
        assertThat(output.getRows().get(0).get("name"), is("first"));
        assertThat(output.getRows().get(1).get("name"), is("second"));
        assertThat(output.getRows().get(2), hasKey("tables"));
    }

    @Test
    void regression_SelectAndDefineTable() throws Exception {
        RunContext runContext = runContextFactory.of();
        String table = "table_reg_" + UUID.randomUUID().toString().toLowerCase().replace("-", "");

        Query.Output defineOutput = authentifiedQueryBuilder()
            .query("DEFINE TABLE %s SCHEMAFULL; DEFINE FIELD name ON %s TYPE string;".formatted(table, table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);
        assertThat(defineOutput.getSize(), is(0L));
        assertThat(defineOutput.getRow(), nullValue());
        assertThat(defineOutput.getRows(), nullValue());

        Query.Output selectEmpty = authentifiedQueryBuilder()
            .query("SELECT * FROM %s".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);
        assertThat(selectEmpty.getSize(), is(0L));
        assertThat(selectEmpty.getRows(), hasSize(0));

        authentifiedQueryBuilder()
            .query("CREATE %s:1 SET name='sample'".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        Query.Output selectPopulated = authentifiedQueryBuilder()
            .query("SELECT * FROM %s".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);
        assertThat(selectPopulated.getSize(), is(1L));
        assertThat(selectPopulated.getRow().get("name"), is("sample"));
    }

    @Test
    void edgeCases_EmptyNullNestedAndSelectValue() throws Exception {
        RunContext runContext = runContextFactory.of();

        // RETURN [] -> 0 rows
        Query.Output returnEmptyArray = authentifiedQueryBuilder()
            .query("RETURN []")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);
        assertThat(returnEmptyArray.getSize(), is(0L));
        assertThat(returnEmptyArray.getRows(), hasSize(0));

        // RETURN NONE -> 0 rows
        Query.Output returnNone = authentifiedQueryBuilder()
            .query("RETURN NONE")
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);
        assertThat(returnNone.getSize(), is(0L));
        assertThat(returnNone.getRow(), nullValue());

        // RETURN NULL -> 0 rows
        Query.Output returnNull = authentifiedQueryBuilder()
            .query("RETURN NULL")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);
        assertThat(returnNull.getSize(), is(0L));
        assertThat(returnNull.getRows(), hasSize(0));

        // Nested arrays -> each inner array is wrapped as {"result": [...]}
        Query.Output returnNested = authentifiedQueryBuilder()
            .query("RETURN [[1, 2], [3, 4]]")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);
        assertThat(returnNested.getSize(), is(2L));
        assertThat(returnNested.getRows(), hasSize(2));
        Map<String, Object> firstNestedRow = returnNested.getRows().get(0);
        assertThat(firstNestedRow, hasKey("result"));
        assertThat((List<?>) firstNestedRow.get("result"), contains(1.0, 2.0));

        // SELECT VALUE -> each scalar element is wrapped as {"result": <value>}
        String table = "table_sv_" + UUID.randomUUID().toString().toLowerCase().replace("-", "");
        authentifiedQueryBuilder()
            .query("CREATE %s:1 SET c_string='val1'".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        Query.Output selectValue = authentifiedQueryBuilder()
            .query("SELECT VALUE c_string FROM %s".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);
        assertThat(selectValue.getSize(), is(1L));
        assertThat(selectValue.getRows().get(0), hasEntry("result", "val1"));
    }

    // -------------------------------------------------------------------------
    // New tests: SELECT VALUE / scalar arrays with FETCH_ONE, FETCH, STORE
    // -------------------------------------------------------------------------

    @Test
    void selectValue_FetchOne_ReturnsWrappedFirstElement() throws Exception {
        RunContext runContext = runContextFactory.of();

        // RETURN ['x','y'] FETCH_ONE -> first element wrapped as {"result": "x"}
        Query.Output output = authentifiedQueryBuilder()
            .query("RETURN ['x', 'y']")
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRow(), hasEntry("result", "x"));
    }

    @Test
    void selectValue_FetchOne_SelectValueFromTable() throws Exception {
        RunContext runContext = runContextFactory.of();
        String table = "table_sv2_" + UUID.randomUUID().toString().toLowerCase().replace("-", "");

        authentifiedQueryBuilder()
            .query("CREATE %s:1 SET name='alice'".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        // SELECT VALUE name FROM t FETCH_ONE -> first element is "alice", wrapped as {"result": "alice"}
        Query.Output output = authentifiedQueryBuilder()
            .query("SELECT VALUE name FROM %s".formatted(table))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRow(), hasEntry("result", "alice"));
    }

    @Test
    void scalarArray_Fetch_WrapsEachElement() throws Exception {
        RunContext runContext = runContextFactory.of();

        // RETURN [10, 20, 30] FETCH -> three wrapped rows
        Query.Output output = authentifiedQueryBuilder()
            .query("RETURN [10, 20, 30]")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);

        assertThat(output.getSize(), is(3L));
        List<Map<String, Object>> rows = output.getRows();
        assertThat(rows, hasSize(3));
        assertThat(rows.get(0), hasEntry("result", 10.0));
        assertThat(rows.get(1), hasEntry("result", 20.0));
        assertThat(rows.get(2), hasEntry("result", 30.0));
    }

    @Test
    void scalarArray_Store_WrapsEachElement() throws Exception {
        RunContext runContext = runContextFactory.of();

        // RETURN ['a', 'b'] STORE -> two wrapped rows in storage
        Query.Output output = authentifiedQueryBuilder()
            .query("RETURN ['a', 'b']")
            .fetchType(Property.ofValue(FetchType.STORE))
            .build().run(runContext);

        assertThat(output.getSize(), is(2L));

        // Read all lines from the stored ION file
        String fileContent = IOUtils.toString(
            storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()), Charsets.UTF_8
        );
        // ION file has one JSON/ION object per line; read each line
        String[] lines = fileContent.trim().split("\n");
        assertThat(lines.length, is(2));
        Map<?, ?> row0 = JacksonMapper.ofIon().readValue(lines[0], Map.class);
        Map<?, ?> row1 = JacksonMapper.ofIon().readValue(lines[1], Map.class);
        assertThat(row0.get("result"), is("a"));
        assertThat(row1.get("result"), is("b"));
    }

    @Test
    void nestedArray_WrapsInnerArraysAsResult() throws Exception {
        RunContext runContext = runContextFactory.of();

        // RETURN [[1,2]] -> one row: {"result": [1.0, 2.0]}
        Query.Output output = authentifiedQueryBuilder()
            .query("RETURN [[1,2]]")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);

        assertThat(output.getSize(), is(1L));
        Map<String, Object> row = output.getRows().get(0);
        assertThat(row, hasKey("result"));
        assertThat((List<?>) row.get("result"), contains(1.0, 2.0));
    }

    @Test
    void mixedArray_ObjectsAndScalarsNormalized() throws Exception {
        RunContext runContext = runContextFactory.of();

        // RETURN [1, {a: 1}] -> row 0: {"result": 1.0}, row 1: {"a": 1.0}
        Query.Output output = authentifiedQueryBuilder()
            .query("RETURN [1, {a: 1}]")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);

        assertThat(output.getSize(), is(2L));
        List<Map<String, Object>> rows = output.getRows();
        assertThat(rows.get(0), hasEntry("result", 1.0));
        assertThat(rows.get(1), hasEntry("a", 1.0));
        assertThat(rows.get(1), not(hasKey("result")));
    }

    @Test
    void nullElement_WrappedAsResultNull() throws Exception {
        RunContext runContext = runContextFactory.of();

        // RETURN [1, NULL] (SurrealDB: NONE is null at driver level) ->
        // row 0: {"result": 1.0}, row 1: {"result": null}
        Query.Output output = authentifiedQueryBuilder()
            .query("RETURN [1, NONE]")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContext);

        assertThat(output.getSize(), is(2L));
        List<Map<String, Object>> rows = output.getRows();
        assertThat(rows.get(0), hasEntry("result", 1.0));
        // null element: key present, value is null
        assertThat(rows.get(1), hasKey("result"));
        assertThat(rows.get(1).get("result"), nullValue());
    }

    @Test
    void query_ErrorStatusThrowsSurrealException() {
        RunContext runContext = runContextFactory.of();

        Query query = authentifiedQueryBuilder()
            .query("THROW 'custom error message';")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        SurrealException exception = assertThrows(SurrealException.class, () -> query.run(runContext));
        assertThat(exception.getMessage(), containsString("custom error message"));
    }

    private static Map<Object, Object> toMap(List<Object> list) {
        return list.stream()
            .flatMap(entry -> ((Map<String, String>) entry).entrySet().stream())
            .collect(TreeMap::new, (hashMap, entry) -> hashMap.put(entry.getKey(), entry.getValue()), TreeMap::putAll);
    }
}
