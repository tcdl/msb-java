package io.github.tcdl.acceptance;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by rdrozdov-tc on 6/16/15.
 */
public class RequesterResponderRunner {

    @Test
    public void runTest() throws Exception {
        RequesterResponderTest test = new RequesterResponderTest();
        test.runRequesterResponder();
        Thread.sleep(3000);

        assertTrue(test.isPassed());
    }
}
