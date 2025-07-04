package io.kestra.plugin.surrealdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow if a periodically executed SurrealDB query returns a non-empty result set."
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
                        format: "{{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.surrealdb.Trigger
                    interval: "PT5M"
                    host: localhost
                    port: 8000
                    username: surreal_user
                    password: surreal_passwd
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

    private Property<String> username;

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

    @Builder.Default
    protected final Duration interval = Duration.ofMinutes(1);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Query.Output queryOutput = Query.builder()
            .host(host)
            .namespace(namespace)
            .database(database)
            .query(query)
            .parameters(parameters)
            .fetchType(fetchType)
            .password(password)
            .username(username)
            .build().run(runContext);

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

}
