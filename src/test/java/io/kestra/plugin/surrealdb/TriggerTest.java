package io.kestra.plugin.surrealdb;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@KestraTest
public class TriggerTest extends SurrealDBTest {

    @SuppressWarnings("unchecked")
    @Test
    @EvaluateTrigger(flow = "flows/surrealdb-listen.yml", triggerId = "watch")
    void simpleQueryTrigger(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();
        Map<String, Object> row = (Map<String, Object>) execution.getTrigger().getVariables().get("row");
        assertThat(row.get("c_string"), is("A collection doc"));
    }
}
