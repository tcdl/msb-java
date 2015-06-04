package io.github.tcdl;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;

/**
 * Created by anstr on 5/26/2015.
 */
public class ResponderTest {

    private MsbMessageOptions config;
    private MsbConfigurations msbConf;
    private static final String TOPIC = "test:responder";

    private ChannelManager mockChannelManager;
    private Producer mockProducer;
    private Payload payload;
    private Message originalMessage;
    private Responder responder;

    @Before
    public void setUp() {
        config = TestUtils.createSimpleConfig();

        msbConf = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        MessageFactory messageFactory = new MessageFactory(msbConf.getServiceDetails(), clock);
        MessageFactory spyMessageFactory = spy(messageFactory);

        MsbContext msbContext = TestUtils.createSimpleMsbContext();
        MsbContext msbContextSpy = spy(msbContext);
        mockChannelManager = mock(ChannelManager.class);
        mockProducer = mock(Producer.class);
        payload = new Payload.PayloadBuilder().build();
        originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);

        when(msbContextSpy.getChannelManager()).thenReturn(mockChannelManager);
        when(msbContextSpy.getMessageFactory()).thenReturn(spyMessageFactory);
        when(mockChannelManager.findOrCreateProducer(anyString())).thenReturn(mockProducer);

        responder = new Responder(config, originalMessage, msbContextSpy);
    }

    @Test
    public void testResponderConstructorOk() {
        MsbContext context = new MsbContext.MsbContextBuilder().build();
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        new Responder(config, originalMessage, context);
    }

    @Test
    public void testProducerWasCreatedForProperTopic() {
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        responder.send(payload);

        verify(mockChannelManager).findOrCreateProducer(argument.capture());

        assertEquals(originalMessage.getTopics().getResponse(), argument.getValue());
    }

    @Test
    public void testProducerPublishMethodInvoked() {
        responder.send(payload);

        verify(mockProducer, times(1)).publish(anyObject());
    }

    @Test
    public void testProducerPublishUseCorrectPayload() {
        ArgumentCaptor<Message> argument = ArgumentCaptor.forClass(Message.class);
        responder.send(payload);

        verify(mockProducer).publish(argument.capture());

        assertEquals(payload, argument.getValue().getPayload());
    }

    @Test
    public void testAckBuilderContainsCorrectProperties() {
        Integer timeout = Integer.valueOf(222);
        Integer responsesRemaining = Integer.valueOf(2);
        ArgumentCaptor<Message> argument = ArgumentCaptor.forClass(Message.class);
        responder.sendAck(timeout, responsesRemaining);

        verify(mockProducer).publish(argument.capture());

        assertEquals(argument.getValue().getAck().getTimeoutMs(), timeout);
        assertEquals(argument.getValue().getAck().getResponsesRemaining(), responsesRemaining);
    }
}
