package io.github.tcdl.msb.acceptance;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RequesterResponderRunner {

    @Test
    public void runTest() throws Exception {
        RequesterResponderTest test = new RequesterResponderTest();
        test.runRequesterResponder();

        assertTrue(test.isPassed());
    }
}
