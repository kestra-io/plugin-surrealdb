package io.kestra.plugin.surrealdb;

import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.Label;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.RunContextInitializer;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class TriggerTest extends SurrealDBTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private RunContextInitializer runContextInitializer;

    @ParameterizedTest
    @ValueSource(strings = { "", "port: 8000", "port: \"{{ vars.port }}\"" })
    void triggerWithPortFromYaml(String port) throws Exception {
        var flow = YamlParser.parse(
            """
                id: %s
                namespace: io.kestra.tests
                variables:
                  port: "8000"
                tasks:
                  - id: end
                    type: io.kestra.plugin.core.debug.Return
                    format: done
                triggers:
                  - id: watch
                    type: io.kestra.plugin.surrealdb.Trigger
                    host: 127.0.0.1
                    %s
                    username: Administrator
                    password: password
                    namespace: some-namespace
                    database: some-database
                    fetchType: FETCH_ONE
                    query: "RETURN [{ source: 'port_test' }]"
                """.formatted(IdUtils.create(), port), Flow.class
        ).toBuilder().tenantId(MAIN_TENANT).build();

        var execution = evaluate(flow.getTriggers().getFirst(), flow).orElseThrow();

        assertThat(execution.getTrigger().getVariables().get("size"), is(1L));
        assertThat((Map<String, Object>) execution.getTrigger().getVariables().get("row"), hasEntry("source", "port_test"));
    }

    @SuppressWarnings("unchecked")
    @Test
    @EvaluateTrigger(flow = "flows/surrealdb-listen.yml", triggerId = "watch")
    void simpleQueryTrigger(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();
        Map<String, Object> row = (Map<String, Object>) execution.getTrigger().getVariables().get("row");
        assertThat(row.get("c_string"), is("A collection doc"));

        assertThat(execution.getId(), is(notNullValue()));
        assertThat(execution.getId(), not(is("watch")));
        assertThat(execution.getTenantId(), is(MAIN_TENANT));
        assertThat(execution.getLabels().stream().map(Label::key).toList(), hasItem(Label.CORRELATION_ID));
    }

    @Test
    void generatedExecutionCarriesFlowContext() throws Exception {
        var flow = loadFlow("flows/surrealdb-listen.yml").toBuilder().revision(3).build();
        var trigger = flow.getTriggers().stream()
            .filter(t -> t.getId().equals("watch"))
            .findFirst()
            .orElseThrow();

        var execution = evaluate(trigger, flow).orElseThrow();

        assertThat(execution.getId(), is(notNullValue()));
        assertThat(execution.getId(), not(is("watch")));
        assertThat(execution.getTenantId(), is(MAIN_TENANT));
        assertThat(execution.getFlowRevision(), is(3));
        assertThat(execution.getLabels().stream().map(Label::key).toList(), hasItem(Label.CORRELATION_ID));
    }

    @Test
    void distinctExecutionIdsAcrossEvaluations() throws Exception {
        var flow = loadFlow("flows/surrealdb-listen.yml");
        var trigger = flow.getTriggers().stream()
            .filter(t -> t.getId().equals("watch"))
            .findFirst()
            .orElseThrow();

        var first = evaluate(trigger, flow).orElseThrow();
        var second = evaluate(trigger, flow).orElseThrow();

        assertThat(first.getId(), not(is(second.getId())));
    }

    private Flow loadFlow(String path) throws Exception {
        var url = getClass().getClassLoader().getResource(path);
        var flow = YamlParser.parse(Paths.get(url.toURI()).toFile(), Flow.class);
        if (flow.getTenantId() == null) {
            flow = flow.toBuilder().tenantId(MAIN_TENANT).build();
        }
        return flow;
    }

    private Optional<Execution> evaluate(AbstractTrigger trigger, Flow flow) throws Exception {
        var triggerContext = TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .tenantId(flow.getTenantId())
            .build();

        var conditionContext = ConditionContext.builder()
            .runContext(
                runContextInitializer.forScheduler(
                    (DefaultRunContext) runContextFactory.of(flow, trigger), triggerContext, trigger
                )
            )
            .flow(flow)
            .build();

        return ((PollingTriggerInterface) trigger).evaluate(conditionContext, triggerContext);
    }

    @Test
    void triggerHonorsConfiguredPort() {
        RunContext runContext = runContextFactory.of();

        Trigger trigger = Trigger.builder()
            .id("watch")
            .host(HOST)
            .port(Property.ofValue(1))
            .namespace(NAMESPACE)
            .database(DATABASE)
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .query("SELECT * FROM testtable_port")
            .build();

        ConditionContext conditionContext = ConditionContext.builder().runContext(runContext).build();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("surrealdb-port")
            .triggerId("watch")
            .build();

        Executable evaluate = () -> trigger.evaluate(conditionContext, triggerContext);
        assertThrows(Exception.class, evaluate);
    }

    @Test
    void triggerWithNonListStatement() throws Exception {
        var flow = YamlParser.parse(
            """
                id: %s
                namespace: io.kestra.tests
                tasks:
                  - id: end
                    type: io.kestra.plugin.core.debug.Return
                    format: done
                triggers:
                  - id: watch
                    type: io.kestra.plugin.surrealdb.Trigger
                    host: 127.0.0.1
                    username: Administrator
                    password: password
                    namespace: some-namespace
                    database: some-database
                    fetchType: FETCH_ONE
                    query: "INFO FOR DB"
                """.formatted(IdUtils.create()), Flow.class
        ).toBuilder().tenantId(MAIN_TENANT).build();

        var execution = evaluate(flow.getTriggers().getFirst(), flow).orElseThrow();

        assertThat(execution.getTrigger().getVariables().get("size"), is(1L));
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) execution.getTrigger().getVariables().get("row");
        assertThat(row, hasKey("tables"));
    }
}
