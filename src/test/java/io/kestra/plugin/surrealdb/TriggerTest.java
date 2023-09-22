package io.kestra.plugin.surrealdb;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.core.services.FlowListenersInterface;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@MicronautTest
public class TriggerTest extends SurrealDBTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListenersInterface flowListeners;

    @Inject
    private SchedulerTriggerStateInterface triggerState;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;

    @Test
    void simpleQueryTrigger() throws Exception {
        Execution execution = triggerFlow();

        Map<String, Object> row = (Map<String, Object>) execution.getTrigger().getVariables().get("row");
        assertThat(row.get("c_string"), is("A collection doc"));
    }

    private Execution triggerFlow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);

        Worker worker = new Worker(applicationContext, 8, null);

        try (AbstractScheduler scheduler = new DefaultScheduler(this.applicationContext, this.flowListeners, this.triggerState)) {
            AtomicReference<Execution> last = new AtomicReference<>();

            Runnable receive = executionQueue.receive("surrealdb-listen", execution -> {
                last.set(execution.getLeft());

                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("surrealdb-listen"));
            });

            worker.run();
            scheduler.run();

            localFlowRepositoryLoader.load(this.getClass().getClassLoader().getResource("flows/surrealdb-listen.yml"));

            boolean await = queueCount.await(1, TimeUnit.MINUTES);

            return last.get();
        }
    }

}
