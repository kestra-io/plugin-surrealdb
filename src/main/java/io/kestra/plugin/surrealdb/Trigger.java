package io.kestra.plugin.surrealdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.surrealdb.connection.SurrealWebSocketConnection;
import com.surrealdb.connection.exception.SurrealException;
import com.surrealdb.driver.AsyncSurrealDriver;
import com.surrealdb.driver.SyncSurrealDriver;
import com.surrealdb.driver.model.QueryResult;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger Flow when SurrealDB query returns rows",
    description = "Polls a SurrealQL query on a fixed interval (default 1 minute) and starts the Flow when it returns at least one row. Defaults to `fetchType: STORE`, which uploads results to internal storage; use `FETCH` or `FETCH_ONE` to surface rows on `trigger`. TLS is disabled by default; keep credentials in secrets."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for SurrealQL query to return results, and then iterate through rows.",
            full = true,
            code = """
                id: surrealdb_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ fromJson(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.surrealdb.Trigger
                    interval: "PT5M"
                    host: localhost
                    port: 8000
                    username: surreal_user
                    password: "{{ secret('SURREALDB_PASSWORD') }}"
                    namespace: surreal_namespace
                    database: surreal_db
                    fetchType: FETCH
                    query: SELECT * FROM SURREAL_TABLE
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, SurrealDBConnectionInterface, QueryInterface {

    @Builder.Default
    private Property<Boolean> useTls = Property.ofValue(false);

    @Positive
    @Builder.Default
    private int port = 8000;

    @NotBlank
    private String host;

    @ToString.Exclude
    private Property<String> username;

    @ToString.Exclude
    private Property<String> password;

    @NotBlank
    private String namespace;

    @NotBlank
    private String database;

    @Positive
    @Builder.Default
    private int connectionTimeout = 60;

    @NotNull
    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Builder.Default
    protected Property<Map<String, String>> parameters = Property.ofValue(new HashMap<>());

    @NotBlank
    protected String query;

    @Schema(
        title = "Polling interval",
        description = "Time between query executions; default 1 minute."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected final Duration interval = Duration.ofMinutes(1);

    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Builder.Default
    private transient AtomicReference<CompletableFuture<?>> activeQuery = new AtomicReference<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Query.Output queryOutput = runQuery(runContext);

        logger.debug("Found '{}' rows from '{}'", queryOutput.getSize(), runContext.render(this.query));

        if (queryOutput.getSize() == 0) {
            return Optional.empty();
        }

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(this, queryOutput);

        Execution execution = Execution.builder()
            .id(id)
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }

    @Override
    public void kill() {
        CompletableFuture<?> future = activeQuery.get();
        if (future != null) {
            future.cancel(true);
        }
    }

    private Query.Output runQuery(RunContext runContext) throws Exception {
        // Preserve the previous delegation behavior exactly: Trigger.evaluate()
        // built a Query without port/useTls/connectionTimeout, so the Query
        // defaults (8000/false/60) applied. Do not substitute this trigger's
        // own port/useTls/connectionTimeout here; that would be an unrelated
        // behavior change outside the scope of kill().
        SurrealWebSocketConnection connection = new SurrealWebSocketConnection(
            runContext.render(host),
            8000,
            false
        );
        connection.connect(60);

        SyncSurrealDriver setupDriver = new SyncSurrealDriver(connection);
        if (username != null && password != null) {
            setupDriver.signIn(
                runContext.render(username).as(String.class).orElseThrow(),
                runContext.render(password).as(String.class).orElseThrow()
            );
        }
        setupDriver.use(runContext.render(namespace), runContext.render(database));

        String renderedQuery = runContext.render(query);
        Map<String, String> parametersValue = runContext.render(parameters).asMap(String.class, String.class).isEmpty() ? new HashMap<>()
            : runContext.render(parameters).asMap(String.class, String.class);

        AsyncSurrealDriver driver = new AsyncSurrealDriver(connection);
        CompletableFuture<List<QueryResult<Object>>> future = driver.query(renderedQuery, parametersValue, Object.class);
        activeQuery.set(future);
        try {
            List<QueryResult<Object>> results = getResultSynchronously(future);

            Query.Output.OutputBuilder outputBuilder = Query.Output.builder().size(
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
            clearActiveQuery(future);
            connection.close();
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

    void clearActiveQuery(CompletableFuture<?> future) {
        activeQuery.compareAndSet(future, null);
    }

    private Stream<Map<String, Object>> getResultStream(List<QueryResult<Object>> results) {
        return results.stream()
            .map(QueryResult::getResult)
            .filter(Objects::nonNull)
            .flatMap(list -> list.stream().map(object -> (Map<String, Object>) object));
    }

    private URI getTempFile(RunContext runContext, List<Map<String, Object>> results) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
            var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var flux = Flux.fromIterable(results);
            FileSerde.writeAll(output, flux).block();
            fileWriter.flush();
        }

        return runContext.storage().putFile(tempFile);
    }

}
