package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.Payload;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleRequester {

    private static final Integer NUMBER_OF_RESPONSES = 1;

    private CountDownLatch passedLatch;
    private String namespace;
    private MsbTestHelper helper = MsbTestHelper.getInstance();

    SimpleRequester(String namespace) {
        this.namespace = namespace;
    }

    public boolean isPassed() {
        try {
            passedLatch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }

        return passedLatch.getCount() == 0;
    }

    public void runSimpleRequesterExample(String... expectedResponses) throws Exception {
        Requester<Payload<Object, Object, Object, Map<String, Object>>> requester = helper.createRequester(namespace, NUMBER_OF_RESPONSES);

        passedLatch = new CountDownLatch(expectedResponses != null ? expectedResponses.length : 0);

        helper.sendRequest(requester, NUMBER_OF_RESPONSES, response -> {
            String body = response.getBody().toString();
            for (String bodyFragment : expectedResponses) {
                if (body.contains(bodyFragment)) {
                    passedLatch.countDown();
                }
            }
        });
    }

    public void shutdown() {
        helper.shutdown();
    }
}
