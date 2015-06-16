package io.github.tcdl.examples;

import io.github.tcdl.Requester;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class RequesterResponderTest extends BaseExample {

    final String NAMESPACE = "test:requester-responder-example";

    @Test
    public void run() throws Exception {
        // running responder server
        createResponderServer(NAMESPACE)
                .use(((request, responder) -> {
                    responder.sendAck(1000, 1);
                    respond(responder);
                }))
                .listen();

        // sending a request
        Requester requester = createRequester(NAMESPACE, 1);

        CountDownLatch await = new CountDownLatch(2);

        sendRequest(requester, "RequesterResponderTest:request", true, 1, acknowledge -> await.countDown(), payload -> await.countDown());

        await.await(5, TimeUnit.SECONDS);

        assertEquals("No acknowledge or response received", 0, await.getCount());
    }
}
