package io.github.tcdl.acceptance;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.github.tcdl.acceptance.TestConfig.TIMEOUT_IN_SECONDS;
import static org.junit.Assert.assertTrue;
/**
 * Created by rdrozdov-tc on 6/16/15.
 */
public class RequesterResponderRunner {

    @Test
    public void runTest() throws Exception {
        RequesterResponderTest test = new RequesterResponderTest();
        test.runRequesterResponder();

        TimeUnit.SECONDS.sleep(TIMEOUT_IN_SECONDS);

        assertTrue(test.isPassed());
    }
}
