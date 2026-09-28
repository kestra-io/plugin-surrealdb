package io.kestra.plugin.surrealdb;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.surrealdb.connection.SurrealWebSocketConnection;
import com.surrealdb.connection.exception.SurrealException;
import com.surrealdb.driver.AsyncSurrealDriver;
import com.surrealdb.driver.SyncSurrealDriver;
import com.surrealdb.driver.model.QueryResult;

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
    description = "Executes a SurrealQL statement against a SurrealDB database. Defaults to `fetchType: STORE`, which streams rows to internal storage; use `FETCH` or `FETCH_ONE` to surface rows directly. TLS is off by default; keep credentials in secrets."
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
        )
    }
)
public class Query extends SurrealDBConnection implements RunnableTask<Query.Output>, QueryInterface {

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
            getPort(),
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

            AsyncSurrealDriver driver = new AsyncSurrealDriver(connection);
            CompletableFuture<List<QueryResult<Object>>> future = driver.query(renderedQuery, parametersValue, Object.class);
            onSubmit.accept(future);
            List<QueryResult<Object>> results = getResultSynchronously(future);

            Query.Output.OutputBuilder outputBuilder = Output.builder().size(
                results.stream()
                    .mapToLong(result -> result.getResult() != null ? (long) result.getResult().size() : (long) 0)
                    .sum()
            );

            return (switch (runContext.render(fetchType).as(FetchType.class).orElseThrow()) {
                case FETCH -> outputBuilder.rows(getResultStream(results).toList());
                case FETCH_ONE -> outputBuilder.row(getResultStream(results).findFirst().orElse(null));
                case STORE -> outputBuilder.uri(getTempFile(runContext, getResultStream(results).toList()));
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

    private Stream<Map<String, Object>> getResultStream(List<QueryResult<Object>> results) {
        return results.stream()
            .map(QueryResult::getResult)
            .filter(Objects::nonNull)
            .flatMap(list -> list.stream().map(object -> (Map<String, Object>) object));
    }

    private URI getTempFile(RunContext runContext, List<Map<String, Object>> results) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            var flux = Flux.fromIterable(results);
            FileSerde.writeAll(output, flux).block();
        }

        return runContext.storage().putFile(tempFile);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "All fetched rows",
            description = "Populated only when `fetchType: FETCH`."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First fetched row",
            description = "Populated only when `fetchType: FETCH_ONE`."
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
