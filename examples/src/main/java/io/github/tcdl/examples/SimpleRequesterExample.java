package io.github.tcdl.examples;

import io.github.tcdl.Requester;

import static org.junit.Assert.assertTrue;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleRequesterExample extends BaseExample {

    private boolean passed;

    private String namespace;

    SimpleRequesterExample(String namespace) {
        this.namespace = namespace;
    }

    public boolean isPassed() {
        return passed;
    }

    public void runSimpleRequesterExample() throws Exception {
        Requester requester = createRequester(namespace, 1);
        sendRequest(requester, 1, response -> {
            try {
                //assertTrue(response.getBody().toString().contains("test:simple-queue2"));
                //assertTrue(response.getBody().toString().contains("test:simple-queue3"));
                passed = true;
            } catch (Exception e) {
                passed = false;
                e.printStackTrace();
            }
        });
    }
}
