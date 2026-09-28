package io.kestra.plugin.surrealdb;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * Kills a polling trigger blocked in a real {@code RETURN SLEEP(20s)} query
 * against the repository's SurrealDB test instance and verifies the
 * evaluation exits promptly as no execution.
 *
 * <p>
 * Only client-side unblocking is verified: the waiting thread must exit
 * well before the 20-second query finishes. No claim is made about
 * server-side query cancellation, which driver 0.1.0 does not support.
 */
@KestraTest
class TriggerKillIntegrationTest extends SurrealDBTest {

    private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(15);

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void killedTriggerExitsBlockedQueryAsNoExecution() throws Exception {
        RunContext runContext = runContextFactory.of();

        Trigger trigger = Trigger.builder()
            .id("watch")
            .host(HOST)
            .namespace(NAMESPACE)
            .database(DATABASE)
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .query("RETURN SLEEP(20s)")
            .build();

        ConditionContext conditionContext = ConditionContext.builder().runContext(runContext).build();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("surrealdb-kill")
            .triggerId("watch")
            .build();

        AtomicReference<CompletableFuture<?>> activeQuery = trigger.activeQueryReference();
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "polling-thread"));
        try {
            Future<Optional<Execution>> evaluation = executor.submit(() -> trigger.evaluate(conditionContext, triggerContext));

            long publishDeadline = System.currentTimeMillis() + PUBLISH_TIMEOUT.toMillis();
            while (activeQuery.get() == null && System.currentTimeMillis() < publishDeadline) {
                if (evaluation.isDone()) {
                    break;
                }
                Thread.sleep(100);
            }
            assertThat("evaluation must reach the in-flight query stage", activeQuery.get() != null, is(true));

            Instant killStart = Instant.now();
            trigger.kill();
            Optional<Execution> result;
            try {
                result = evaluation.get(EXIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError("evaluation remained blocked after kill()", e);
            }
            long killElapsed = Duration.between(killStart, Instant.now()).toMillis();

            assertThat("cancelled poll must be no execution, not a failure", result.isPresent(), is(false));
            assertThat(killElapsed, lessThan(EXIT_TIMEOUT.toMillis()));
            assertThat(activeQuery.get(), is(nullValue()));
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS), is(true));
        }
    }
}
