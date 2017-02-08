package io.github.tcdl.msb.acceptance;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.api.ResponderServer;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test checks the case when {@link MsbContext} in shut down while message is being processed.
 * It ensures that if message processing is successful than acknowledgement is sent to the broker.
 * <p>
 * Steps being performed:
 * <ol>
 * <li>set up MsbContext with durable AMQP queues and exchanges</li>
 * <li>ensure incoming queue bound to <code>test:shutdown</code> exchange is empty by consuming everything from it</li>
 * <li>start ResponderServer with blocking message handler (to suspend processing)</li>
 * <li>publish test message to invoke blocking handler</li>
 * <li>call {@link MsbContext#shutdown()}</li>
 * <li>unblock message handler</li>
 * <li>check that handler was successfully executed</li>
 * <li>run ResponderServer from new 'probe' MsbContext that generates the same queue names as the previous one and check that there are no messages in incoming queue</li>
 * </ol>
 * There are several arbitrary delays between some steps because of reactive non-blocking API of msb-java.
 *
 * @author Alexandr Zolotov
 */
public class ShutdownTest {

    private static final Logger LOG = LoggerFactory.getLogger(ShutdownTest.class);

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private Config config = ConfigFactory.load();

    @Test(timeout = 10000)
    public void testShutdown() throws Exception {

        String namespace = "test:shutdown";
        AtomicBoolean handlerFinished = new AtomicBoolean(false);
        CountDownLatch handlerBlockingLatch = new CountDownLatch(1);

        MsbContext msbContext = createMsbContext(config);
        emptyIncomingQueue(namespace, msbContext).get(); //block until queue is empty

        TimeUnit.MILLISECONDS.sleep(50);
        msbContext.getObjectFactory().createResponderServer(namespace, ResponderOptions.DEFAULTS,
                (request, responderContext) -> {
                    handlerBlockingLatch.await();
                    handlerFinished.set(true);
                },
                String.class)
                .listen();

        TimeUnit.MILLISECONDS.sleep(50);

        msbContext.getObjectFactory()
                .createRequesterForFireAndForget(namespace)
                .publish("very important message");

        executor.schedule(handlerBlockingLatch::countDown, 200, TimeUnit.MILLISECONDS);
        //give msb context some time to be sure it received request before shutdown
        TimeUnit.MILLISECONDS.sleep(50);
        msbContext.shutdown();

        //verify handler was executed
        assertTrue(handlerFinished.get());
        MsbContext probeContext = createMsbContext(config);

        CountDownLatch messageReceivedFromQueueLatch = new CountDownLatch(1);

        probeContext.getObjectFactory().createResponderServer(namespace, ResponderOptions.DEFAULTS,
                (request, responderContext) -> messageReceivedFromQueueLatch.countDown(), String.class)
                .listen();

        assertFalse("Message is not expected to be present in the queue but it is", messageReceivedFromQueueLatch.await(200, TimeUnit.MILLISECONDS));
    }

    private CompletableFuture<Void> emptyIncomingQueue(String namespace, MsbContext msbContext) {

        int timeoutTillEmptyMs = 1000;
        int oneTickMs = 100;
        int tickMaxCount = timeoutTillEmptyMs / oneTickMs;

        CompletableFuture<Void> handle = new CompletableFuture<>();

        AtomicInteger ticksLeft = new AtomicInteger(tickMaxCount);
        ResponderServer cleanUpServer = msbContext.getObjectFactory().createResponderServer(namespace,
                ResponderOptions.DEFAULTS,
                (request, responderContext) -> {
                    LOG.info("Message received. Resetting the timer.");
                    ticksLeft.set(tickMaxCount);//reset
                },
                String.class
        );

        executor.scheduleWithFixedDelay(() -> {
            if (ticksLeft.decrementAndGet() <= 0) {
                LOG.info("Queue is empty");
                cleanUpServer.stop();
                handle.complete(null);
                throw new RuntimeException("poor man's way to cancel scheduled task");
            }
        }, 100, oneTickMs, TimeUnit.MILLISECONDS);

        cleanUpServer.listen();
        return handle;
    }

    private MsbContext createMsbContext(Config baseConfig) {
        return MsbTestHelper.getInstance().initWithConfig(baseConfig);
    }

    @After
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }

        MsbTestHelper.getInstance().shutdownAll();
    }
}
