package io.kestra.plugin.surrealdb;

import java.net.URL;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

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

import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
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
        Flow flow = loadFlow("flows/surrealdb-listen.yml").toBuilder().revision(3).build();
        AbstractTrigger trigger = flow.getTriggers().stream()
            .filter(t -> t.getId().equals("watch"))
            .findFirst()
            .orElseThrow();

        Execution execution = evaluate(trigger, flow).orElseThrow();

        assertThat(execution.getId(), is(notNullValue()));
        assertThat(execution.getId(), not(is("watch")));
        assertThat(execution.getTenantId(), is(MAIN_TENANT));
        assertThat(execution.getFlowRevision(), is(3));
        assertThat(execution.getLabels().stream().map(Label::key).toList(), hasItem(Label.CORRELATION_ID));
    }

    @Test
    void distinctExecutionIdsAcrossEvaluations() throws Exception {
        Flow flow = loadFlow("flows/surrealdb-listen.yml");
        AbstractTrigger trigger = flow.getTriggers().stream()
            .filter(t -> t.getId().equals("watch"))
            .findFirst()
            .orElseThrow();

        Execution first = evaluate(trigger, flow).orElseThrow();
        Execution second = evaluate(trigger, flow).orElseThrow();

        assertThat(first.getId(), not(is(second.getId())));
    }

    private Flow loadFlow(String path) throws Exception {
        URL url = getClass().getClassLoader().getResource(path);
        Flow flow = YamlParser.parse(Paths.get(url.toURI()).toFile(), Flow.class);
        if (flow.getTenantId() == null) {
            flow = flow.toBuilder().tenantId(MAIN_TENANT).build();
        }
        return flow;
    }

    private Optional<Execution> evaluate(AbstractTrigger trigger, Flow flow) throws Exception {
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .tenantId(flow.getTenantId())
            .build();

        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContextInitializer.forScheduler(
                (DefaultRunContext) runContextFactory.of(flow, trigger), triggerContext, trigger
            ))
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
            .port(1)
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
}
