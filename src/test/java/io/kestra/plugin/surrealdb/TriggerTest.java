package io.kestra.plugin.surrealdb;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class TriggerTest extends SurrealDBTest {

    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    @EvaluateTrigger(flow = "flows/surrealdb-listen.yml", triggerId = "watch")
    void simpleQueryTrigger(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();
        Map<String, Object> row = (Map<String, Object>) execution.getTrigger().getVariables().get("row");
        assertThat(row.get("c_string"), is("A collection doc"));
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
