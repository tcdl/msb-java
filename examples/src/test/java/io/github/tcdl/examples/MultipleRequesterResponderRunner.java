package io.github.tcdl.examples;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Created by anstr on 6/10/2015.
 */

public class MultipleRequesterResponderRunner {
    private static long TIMEOUT_IN_SECONDS = 10 ;

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
        requesterExample.runSimpleRequesterExample();

        TimeUnit.SECONDS.sleep(TIMEOUT_IN_SECONDS);

        responderExample1.shutDown();
        responderExample2.shutDown();
        multipleRequesterResponder.shutDown();
        requesterExample.shutDown();

        assertTrue(requesterExample.isPassed());
    }
}
