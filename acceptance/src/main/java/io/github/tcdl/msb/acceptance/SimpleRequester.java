package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.api.Requester;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
        Requester<String> requester = helper.createRequester(namespace, NUMBER_OF_RESPONSES, String.class);

        passedLatch = new CountDownLatch(expectedResponses != null ? expectedResponses.length : 0);

        helper.sendRequest(requester, "PING", NUMBER_OF_RESPONSES, response -> {
            for (String bodyFragment : expectedResponses) {
                if (response.contains(bodyFragment)) {
                    passedLatch.countDown();
                }
            }
        });
    }

    public void shutdown() {
        helper.shutdown();
    }
}
