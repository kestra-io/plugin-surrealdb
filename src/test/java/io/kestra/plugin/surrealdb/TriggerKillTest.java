package io.kestra.plugin.surrealdb;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Fast lifecycle tests for {@link Trigger#kill()} that verify the trigger's
 * own concurrency contract without requiring a SurrealDB server:
 * null-safe/idempotent kill and identity-based stale-future cleanup.
 * Real cancellation against a live server is covered by
 * {@code TriggerKillIntegrationTest}. No claim is made about server-side
 * query cancellation, which driver 0.1.0 does not support.
 */
class TriggerKillTest {

    private static Trigger newTrigger() {
        return Trigger.builder()
            .id("watch")
            .host("localhost")
            .namespace("ns")
            .database("db")
            .query("SELECT * FROM t")
            .build();
    }

    @Test
    void killWithoutActiveFutureIsSafe() {
        Trigger trigger = newTrigger();
        trigger.kill();
        trigger.kill();
        assertThat(trigger.activeQueryReference().get(), is(nullValue()));
    }

    @Test
    void staleCleanupDoesNotClearNewerFuture() {
        Trigger trigger = newTrigger();
        AtomicReference<CompletableFuture<?>> ref = trigger.activeQueryReference();

        CompletableFuture<String> first = new CompletableFuture<>();
        ref.set(first);
        trigger.clearActiveQuery(first);
        assertThat(ref.get(), is(nullValue()));

        CompletableFuture<String> second = new CompletableFuture<>();
        CompletableFuture<String> stale = new CompletableFuture<>();
        ref.set(second);
        trigger.clearActiveQuery(stale);
        assertThat(ref.get(), is(second));

        trigger.clearActiveQuery(second);
        assertThat(ref.get(), is(nullValue()));
    }

    @Test
    void killAfterCompletionLeavesResultIntact() throws Exception {
        Trigger trigger = newTrigger();
        CompletableFuture<String> future = new CompletableFuture<>();
        trigger.activeQueryReference().set(future);
        future.complete("ok");

        trigger.kill();

        assertThat(future.isCancelled(), is(false));
        assertThat(future.get(), is("ok"));
        trigger.clearActiveQuery(future);
        assertThat(trigger.activeQueryReference().get(), is(nullValue()));
    }
}
