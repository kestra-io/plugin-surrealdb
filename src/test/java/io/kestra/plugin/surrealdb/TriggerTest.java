package io.kestra.plugin.surrealdb;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.Worker;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@KestraTest
public class TriggerTest extends SurrealDBTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListenersInterface flowListeners;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;

    @SuppressWarnings("unchecked")
    @Test
    void simpleQueryTrigger() throws Exception {
        Execution execution = triggerFlow();

        Map<String, Object> row = (Map<String, Object>) execution.getTrigger().getVariables().get("row");
        assertThat(row.get("c_string"), is("A collection doc"));
    }

    private Execution triggerFlow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);

        try (DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);) {
            try (AbstractScheduler scheduler = new JdbcScheduler(this.applicationContext, this.flowListeners)) {
                Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                    queueCount.countDown();
                    assertThat(execution.getLeft().getFlowId(), is("surrealdb-listen"));
                });

                worker.run();
                scheduler.run();

                localFlowRepositoryLoader.load(Objects.requireNonNull(this.getClass()
                    .getClassLoader()
                    .getResource("flows/surrealdb-listen.yml")));

                boolean await = queueCount.await(1, TimeUnit.MINUTES);
                assertThat(await, is(true));

                return receive.blockLast();
            }
        }
    }
}
