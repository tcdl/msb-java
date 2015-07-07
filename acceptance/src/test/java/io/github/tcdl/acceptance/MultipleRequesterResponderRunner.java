package io.github.tcdl.acceptance;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Created by anstr on 6/10/2015.
 */

public class MultipleRequesterResponderRunner {

    private static long TIMEOUT_IN_SECONDS = 5 ;

    @Test
    public void runTest() throws Exception {
        SimpleResponder responderExample1 = new SimpleResponder("test:simple-queue2");
        responderExample1.runSimpleResponderExample();

        SimpleResponder responderExample2 = new SimpleResponder("test:simple-queue3");
        responderExample2.runSimpleResponderExample();

        MultipleRequesterResponder multipleRequesterResponder = new MultipleRequesterResponder(
                "test:simple-queue1",
                "test:simple-queue2",
                "test:simple-queue3");
        multipleRequesterResponder.runMultipleRequesterResponder();

        SimpleRequester requesterExample = new SimpleRequester("test:simple-queue1");
        requesterExample.runSimpleRequesterExample("test:simple-queue2", "test:simple-queue3");

        TimeUnit.SECONDS.sleep(TIMEOUT_IN_SECONDS);

        responderExample1.shutdown();
        responderExample2.shutdown();
        multipleRequesterResponder.shutdown();
        requesterExample.shutdown();

        assertTrue(requesterExample.isPassed());
    }
}
