package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Created by anstr on 6/10/2015.
 */

public class MultipleRequesterResponderRunner {

    @Test
    public void runTest() throws InterruptedException {
        SimpleResponderExample responderExample1 = new SimpleResponderExample(new MsbContext.MsbContextBuilder().build(),
                "test:simple-queue2");
        responderExample1.runSimpleResponderExample();

        SimpleResponderExample responderExample2 = new SimpleResponderExample(new MsbContext.MsbContextBuilder().build(),
                "test:simple-queue3");
        responderExample2.runSimpleResponderExample();

        MultipleRequesterResponder multipleRequesterResponder = new MultipleRequesterResponder(
                new MsbContext.MsbContextBuilder().build(), "test:simple-queue1",
                "test:simple-queue2", "test:simple-queue3");
        multipleRequesterResponder.runMultipleRequesterResponder();

        SimpleRequesterExample requesterExample = new SimpleRequesterExample(new MsbContext.MsbContextBuilder().build(),
                "test:simple-queue1");
        requesterExample.runSimpleRequesterExample();

        TimeUnit.SECONDS.sleep(2);

        responderExample1.getMsbContext().getChannelManager().getAdapterFactory().close();
        responderExample2.getMsbContext().getChannelManager().getAdapterFactory().close();
        multipleRequesterResponder.getMsbContext().getChannelManager().getAdapterFactory().close();
        requesterExample.getMsbContext().getChannelManager().getAdapterFactory().close();

        assertTrue(requesterExample.isPassed());
    }
}
