package io.kestra.plugin.surrealdb;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.surrealdb.connection.ErrorToExceptionMapper;
import com.surrealdb.connection.SurrealWebSocketConnection;
import com.surrealdb.connection.exception.SurrealException;
import com.surrealdb.connection.model.RpcResponse;
import com.surrealdb.driver.SyncSurrealDriver;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a SurrealDB query",
    description = """
        Executes SurrealQL statements against a SurrealDB database.

        Every row in the result is guaranteed to be a `Map<String, Object>`:
        - A statement that returns an array (e.g. `SELECT *`) contributes one row per element.
        - A statement that returns a JSON object (e.g. `INFO FOR DB`, `RETURN {a: 1}`) contributes a single row equal to that object.
        - Any element that is **not** a JSON object — scalars, arrays, or `null` — is wrapped as `{"result": <value>}` so the row type is always a map.

        Multi-statement queries have each statement's results normalized independently and concatenated in order.
        Defaults to `fetchType: STORE`, which streams rows to internal storage; use `FETCH` or `FETCH_ONE` to surface rows directly.
        TLS is off by default; keep credentials in secrets.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SurrealQL query to a SurrealDB database.",
            full = true,
            code = """
                id: surrealdb_query
                namespace: company.team

                tasks:
                  - id: select
                    type: io.kestra.plugin.surrealdb.Query
                    useTls: true
                    host: localhost
                    port: 8000
                    username: surreal_user
                    password: "{{ secret('SURREALDB_PASSWORD') }}"
                    database: surreal_db
                    namespace: surreal_namespace
                    query: SELECT * FROM SURREAL_TABLE
                    fetchType: STORE
                """
        ),
        @Example(
            title = "Inspect database schema information using INFO FOR DB.",
            full = true,
            code = """
                id: surrealdb_info
                namespace: company.team

                tasks:
                  - id: info
                    type: io.kestra.plugin.surrealdb.Query
                    host: localhost
                    port: 8000
                    username: surreal_user
                    password: "{{ secret('SURREALDB_PASSWORD') }}"
                    database: surreal_db
                    namespace: surreal_namespace
                    query: INFO FOR DB
                    fetchType: FETCH_ONE
                """
        )
    }
)
public class Query extends SurrealDBConnection implements RunnableTask<Query.Output>, QueryInterface {

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<Map<String, String>> parameters = Property.ofValue(new HashMap<>());

    @NotBlank
    @PluginProperty(group = "main")
    protected String query;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        return run(runContext, future ->
        {
        });
    }

    /**
     * Executes the SurrealQL query, publishing the in-flight query future to
     * {@code onSubmit} immediately after it is obtained and before blocking
     * on its result, so callers such as the polling trigger can cancel a
     * blocked evaluation from another thread via {@code future.cancel(true)}.
     * Cancelling the future unblocks the waiting thread locally; it does not
     * cancel the server-side query, which driver 0.1.0 does not support.
     */
    public Query.Output run(RunContext runContext, Consumer<CompletableFuture<?>> onSubmit) throws Exception {
        SurrealWebSocketConnection connection = new SurrealWebSocketConnection(
            runContext.render(getHost()),
            runContext.render(getPort()).as(Integer.class).orElse(DEFAULT_PORT),
            runContext.render(getUseTls()).as(Boolean.class).orElseThrow()
        );
        try {
            connection.connect(getConnectionTimeout());

            SyncSurrealDriver setupDriver = new SyncSurrealDriver(connection);
            if (getUsername() != null && getPassword() != null) {
                setupDriver.signIn(
                    runContext.render(getUsername()).as(String.class).orElseThrow(),
                    runContext.render(getPassword()).as(String.class).orElseThrow()
                );
            }
            setupDriver.use(runContext.render(getNamespace()), runContext.render(getDatabase()));

            String renderedQuery = runContext.render(query);

            Map<String, String> parametersValue = runContext.render(parameters).asMap(String.class, String.class);

            Type queryResultType = TypeToken.getParameterized(List.class, QueryResultRaw.class).getType();
            CompletableFuture<List<QueryResultRaw>> future = connection.rpc(queryResultType, "query", renderedQuery, parametersValue);
            onSubmit.accept(future);
            List<QueryResultRaw> results = getResultSynchronously(future);

            List<Map<String, Object>> normalizedRows = results.stream()
                .flatMap(this::normalizeStatement)
                .toList();

            Query.Output.OutputBuilder outputBuilder = Output.builder().size((long) normalizedRows.size());

            return (switch (runContext.render(fetchType).as(FetchType.class).orElseThrow()) {
                case FETCH -> outputBuilder.rows(normalizedRows);
                case FETCH_ONE -> outputBuilder.row(normalizedRows.isEmpty() ? null : normalizedRows.getFirst());
                case STORE -> outputBuilder.uri(getTempFile(runContext, normalizedRows));
                default -> outputBuilder;
            }).build();
        } finally {
            try {
                connection.close();
            } catch (RuntimeException e) {
                runContext.logger().warn("Failed to close SurrealDB connection", e);
            }
        }
    }

    /**
     * Converts a single {@link JsonElement} to a typed row ({@code Map<String, Object>}).
     *
     * <ul>
     * <li>JSON objects are converted field-by-field via Gson into a {@code Map<String, Object>}.</li>
     * <li>Anything else — primitives, arrays, or {@code null} — is wrapped as
     * {@code {"result": <value>}} so that every row has the same Java type.
     * Nested arrays become {@code {"result": [...]}}; JSON null becomes
     * {@code {"result": null}}.</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> toRow(JsonElement element) {
        if (element.isJsonObject()) {
            Type mapType = TypeToken.getParameterized(Map.class, String.class, Object.class).getType();
            return GSON.fromJson(element, mapType);
        }
        // Primitive, array, or null → wrap so callers always get Map<String, Object>
        Object value = element.isJsonNull() ? null : GSON.fromJson(element, Object.class);
        Map<String, Object> row = new HashMap<>();
        row.put("result", value);
        return row;
    }

    private Stream<Map<String, Object>> normalizeStatement(QueryResultRaw raw) {
        if ("ERR".equals(raw.getStatus())) {
            String errorMessage = null;
            if (raw.getDetail() != null && !raw.getDetail().isBlank()) {
                errorMessage = raw.getDetail();
            } else if (raw.getResult() != null) {
                if (raw.getResult().isJsonPrimitive()) {
                    errorMessage = raw.getResult().getAsString();
                } else {
                    errorMessage = raw.getResult().toString();
                }
            }
            if (errorMessage == null || errorMessage.isBlank()) {
                errorMessage = "SurrealDB query statement failed with status ERR";
            }
            RpcResponse.Error rpcError = new RpcResponse.Error();
            rpcError.setMessage(errorMessage);
            throw ErrorToExceptionMapper.map(rpcError);
        }

        JsonElement result = raw.getResult();
        if (result == null || result.isJsonNull()) {
            return Stream.empty();
        }

        if (result.isJsonArray()) {
            JsonArray array = result.getAsJsonArray();
            if (array.isEmpty()) {
                return Stream.empty();
            }
            return StreamSupport.stream(array.spliterator(), false)
                .map(Query::toRow);
        }

        // Non-array top-level result (object or scalar)
        return Stream.of(toRow(result));
    }

    private static <T> T getResultSynchronously(CompletableFuture<T> completableFuture) {
        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SurrealException) {
                throw (SurrealException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private URI getTempFile(RunContext runContext, List<Map<String, Object>> results) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            var flux = Flux.fromIterable(results);
            FileSerde.writeAll(output, flux).block();
        }

        return runContext.storage().putFile(tempFile);
    }

    @Getter
    @NoArgsConstructor
    private static class QueryResultRaw {
        private JsonElement result;
        private String status;
        private String time;
        private String detail;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "All fetched rows",
            description = """
                Populated only when `fetchType: FETCH`.
                Each element is always a `Map<String, Object>`.
                Rows that are JSON objects appear as-is; any row that is not an object
                (scalar, array, or null) is wrapped as `{"result": <value>}`.
                """
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First fetched row",
            description = """
                Populated only when `fetchType: FETCH_ONE`.
                Always a `Map<String, Object>`: JSON-object rows appear as-is;
                non-object rows (scalar, array, or null) are wrapped as `{"result": <value>}`.
                """
        )
        private Map<String, Object> row;

        @Schema(
            title = "URI of stored result",
            description = "Internal storage URI populated only when `fetchType: STORE`."
        )
        private URI uri;

        @Schema(
            title = "Number of rows fetched"
        )
        private Long size;
    }
}
