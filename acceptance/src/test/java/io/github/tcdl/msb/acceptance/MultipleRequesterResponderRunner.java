package io.github.tcdl.msb.acceptance;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
/**
 * Created by anstr on 6/10/2015.
 */

public class MultipleRequesterResponderRunner {

    private SimpleResponder responderExample1;
    private SimpleResponder responderExample2;
    private MultipleRequesterResponder multipleRequesterResponder;
    private SimpleRequester requesterExample;

    @Before
    public void setUp() {
        responderExample1 = new SimpleResponder("test:simple-queue2");
        responderExample2 = new SimpleResponder("test:simple-queue3");
        multipleRequesterResponder = new MultipleRequesterResponder(
                "test:simple-queue1",
                "test:simple-queue2",
                "test:simple-queue3");
        requesterExample = new SimpleRequester("test:simple-queue1");
    }

    @Test
    public void runTest() throws Exception {
        responderExample1.runSimpleResponderExample();
        responderExample2.runSimpleResponderExample();
        multipleRequesterResponder.runMultipleRequesterResponder();
        requesterExample.runSimpleRequesterExample("test:simple-queue2", "test:simple-queue3");

        assertTrue(requesterExample.isPassed());
    }

    @After
    public void tearDown() {
        responderExample1.shutdown();
        responderExample2.shutdown();
        multipleRequesterResponder.shutdown();
        requesterExample.shutdown();
    }
}
