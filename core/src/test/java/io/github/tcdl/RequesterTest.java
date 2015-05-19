package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.support.TestUtils;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by rdro on 4/27/2015.
 */
public class RequesterTest {
    private MsbMessageOptions config;

    @Before
    public void setUp() {
        this.config = TestUtils.createSimpleConfig();
    }

    @Test(expected = NullPointerException.class)
    public void testRequesterNullConfigsThrowsException() {
        new Requester(null, TestUtils.createMsbRequestMessageNoPayload());
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        config.setWaitForResponses(2);

        Requester requester = new Requester(config, TestUtils.createMsbRequestMessageNoPayload());
        requester.publish(TestUtils.createSimpleRequestPayload());
    }

    @Test
    public void testPublishNoWaitForResponses() {
        Requester requester = new Requester(config, TestUtils.createMsbRequestMessageNoPayload());
        requester.publish(TestUtils.createSimpleRequestPayload());
    }

    @Test
    public void testPublishWithoutPayload() {
        Requester requester = new Requester(config, TestUtils.createMsbRequestMessageNoPayload());
        requester.publish(null);
    }
}
