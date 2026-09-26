package io.kestra.plugin.surrealdb;

import java.lang.reflect.Field;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Focused lifecycle tests for {@link Trigger#kill()}.
 *
 * <p>
 * No SurrealDB server required: these tests drive the retained
 * {@code activeQuery} future directly and verify client-side cancellation
 * semantics only. They intentionally make no claim about server-side query
 * cancellation, which driver 0.1.0 does not support.
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

    @SuppressWarnings("unchecked")
    private static AtomicReference<CompletableFuture<?>> activeQueryOf(Trigger trigger) throws Exception {
        Field field = Trigger.class.getDeclaredField("activeQuery");
        field.setAccessible(true);
        return (AtomicReference<CompletableFuture<?>>) field.get(trigger);
    }

    @Test
    void killWithoutActiveFutureIsSafe() {
        Trigger trigger = newTrigger();
        trigger.kill();
        trigger.kill();
    }

    @Test
    void killCancelsActiveFuture() throws Exception {
        Trigger trigger = newTrigger();
        CompletableFuture<String> future = new CompletableFuture<>();
        activeQueryOf(trigger).set(future);

        trigger.kill();

        assertThat(future.isCancelled(), is(true));
        assertThat(future.isDone(), is(true));
    }

    @Test
    void killIsIdempotent() throws Exception {
        Trigger trigger = newTrigger();
        CompletableFuture<String> future = new CompletableFuture<>();
        activeQueryOf(trigger).set(future);

        trigger.kill();
        trigger.kill();

        assertThat(future.isCancelled(), is(true));
    }

    @Test
    void killAfterCompletionDoesNotBreakResult() throws Exception {
        Trigger trigger = newTrigger();
        CompletableFuture<String> future = new CompletableFuture<>();
        activeQueryOf(trigger).set(future);
        future.complete("ok");

        trigger.kill();

        assertThat(future.isCancelled(), is(false));
        assertThat(future.get(5, TimeUnit.SECONDS), is("ok"));
    }

    @Test
    void blockedGetIsUnblockedByKill() throws Exception {
        Trigger trigger = newTrigger();
        CompletableFuture<String> future = new CompletableFuture<>();
        activeQueryOf(trigger).set(future);

        AtomicReference<Throwable> seen = new AtomicReference<>();
        Thread worker = new Thread(() ->
        {
            try {
                future.get();
            } catch (Throwable e) {
                seen.set(e);
            }
        }, "polling-thread");
        worker.start();

        //noinspection StatementWithEmptyBody
        while (!worker.isAlive()) {
        }
        Thread.sleep(200);
        trigger.kill();
        worker.join(5000);

        assertThat(worker.isAlive(), is(false));
        assertThat(seen.get() instanceof CancellationException, is(true));
    }

    @Test
    void clearActiveQueryUsesIdentitySemantics() throws Exception {
        Trigger trigger = newTrigger();
        AtomicReference<CompletableFuture<?>> ref = activeQueryOf(trigger);

        CompletableFuture<String> first = new CompletableFuture<>();
        ref.set(first);
        trigger.clearActiveQuery(first);
        assertThat(ref.get() == null, is(true));

        CompletableFuture<String> second = new CompletableFuture<>();
        CompletableFuture<String> stale = new CompletableFuture<>();
        ref.set(second);
        trigger.clearActiveQuery(stale);
        assertThat(ref.get() == second, is(true));

        trigger.clearActiveQuery(second);
        assertThat(ref.get() == null, is(true));
    }
}
