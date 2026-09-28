package io.kestra.plugin.surrealdb;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.*;
import lombok.experimental.SuperBuilder;

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

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Builder.Default
    private transient AtomicReference<CompletableFuture<?>> activeQuery = new AtomicReference<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Query queryTask = Query.builder()
            .host(host)
            .port(port)
            .useTls(useTls)
            .namespace(namespace)
            .database(database)
            .query(query)
            .parameters(parameters)
            .fetchType(fetchType)
            .password(password)
            .username(username)
            .connectionTimeout(connectionTimeout)
            .build();

        AtomicReference<CompletableFuture<?>> submitted = new AtomicReference<>();
        final Query.Output queryOutput;
        try {
            queryOutput = queryTask.run(runContext, future ->
            {
                submitted.set(future);
                activeQuery.set(future);
            });
        } catch (CancellationException e) {
            logger.debug("SurrealDB polling query for trigger '{}' was cancelled", id);
            return Optional.empty();
        } finally {
            CompletableFuture<?> future = submitted.get();
            if (future != null) {
                clearActiveQuery(future);
            }
        }

        logger.debug("Found '{}' rows from '{}'", queryOutput.getSize(), runContext.render(this.query));

        if (queryOutput.getSize() == 0) {
            return Optional.empty();
        }

        var execution = TriggerService.generateExecution(this, conditionContext, context, queryOutput);

        return Optional.of(execution);
    }

    @Override
    public void kill() {
        CompletableFuture<?> future = activeQuery.get();
        if (future != null) {
            future.cancel(true);
        }
    }

    void clearActiveQuery(CompletableFuture<?> future) {
        activeQuery.compareAndSet(future, null);
    }

    /**
     * Returns the reference holding the currently in-flight query future.
     *
     * <p>
     * Package-private for tests only: it lets integration tests wait
     * deterministically until a poll has reached the in-flight query stage
     * before killing it, and verify cleanup afterwards, without exposing
     * internal cancellation state in the public API.
     */
    AtomicReference<CompletableFuture<?>> activeQueryReference() {
        return activeQuery;
    }

}
