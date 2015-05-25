package io.github.tcdl;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.adapters.MockAdapter;
import io.github.tcdl.adapters.MockAdapterFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.support.TestUtils;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by rdro on 4/27/2015.
 */
public class RequesterTest {

    private MsbMessageOptions config;
    private MessageFactory messageFactory;
    private ChannelManager channelManager;
    private MsbConfigurations msbConfigurations;

    @Before
    public void setUp() {
        this.config = TestUtils.createSimpleConfig();
        this.msbConfigurations = TestUtils.createMsbConfigurations();
        this.messageFactory = new MessageFactory(TestUtils.createMsbConfigurations().getServiceDetails());
        this.channelManager = new ChannelManager(new MockAdapterFactory(ConfigFactory.load()));
    }

    @Test(expected = NullPointerException.class)
    public void testRequesterNullConfigsThrowsException() {
        new Requester(null, TestUtils.createMsbRequestMessageNoPayload(), messageFactory, channelManager, msbConfigurations);
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        config.setWaitForResponses(2);
             
        Requester requester = new Requester(config, TestUtils.createMsbRequestMessageNoPayload(), messageFactory, channelManager, msbConfigurations);
        requester.publish(TestUtils.createSimpleRequestPayload());
    }

    @Test
    public void testPublishNoWaitForResponses() {
        Requester requester = new Requester(config, TestUtils.createMsbRequestMessageNoPayload(), messageFactory, channelManager, msbConfigurations);
        requester.publish(TestUtils.createSimpleRequestPayload());
    }

    @Test
    public void testPublishWithoutPayload() {
        Requester requester = new Requester(config, TestUtils.createMsbRequestMessageNoPayload(), messageFactory, channelManager, msbConfigurations);
        requester.publish(null);
    }
}
