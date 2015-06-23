package io.github.tcdl.examples;

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
        SimpleResponderExample responderExample1 = new SimpleResponderExample("test:simple-queue2");
        responderExample1.runSimpleResponderExample();

        SimpleResponderExample responderExample2 = new SimpleResponderExample("test:simple-queue3");
        responderExample2.runSimpleResponderExample();

        MultipleRequesterResponder multipleRequesterResponder = new MultipleRequesterResponder(
                "test:simple-queue1",
                "test:simple-queue2",
                "test:simple-queue3");
        multipleRequesterResponder.runMultipleRequesterResponder();

        SimpleRequesterExample requesterExample = new SimpleRequesterExample("test:simple-queue1");
        requesterExample.runSimpleRequesterExample("test:simple-queue2", "test:simple-queue3");

        TimeUnit.SECONDS.sleep(TIMEOUT_IN_SECONDS);

        responderExample1.shutdown();
        responderExample2.shutdown();
        multipleRequesterResponder.shutdown();
        requesterExample.shutdown();

        assertTrue(requesterExample.isPassed());
    }
}
