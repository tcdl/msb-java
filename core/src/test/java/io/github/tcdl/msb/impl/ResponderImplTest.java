package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.message.MessageFactory;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ResponderImplTest {

    private MessageTemplate messageTemplate;
    private MsbConfig msbConf;
    private static final String TOPIC = "test:responder";

    private MsbContextImpl msbContextSpy;
    private ChannelManager mockChannelManager;
    private Producer mockProducer;
    private Payload emptyPayload;
    private Message originalMessage;
    private Responder responder;

    @Before
    public void setUp() {
        messageTemplate = TestUtils.createSimpleMessageTemplate();

        msbConf = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        MessageFactory messageFactory = new MessageFactory(msbConf.getServiceDetails(), clock, TestUtils.createMessageMapper());
        MessageFactory spyMessageFactory = spy(messageFactory);

        MsbContextImpl msbContext = TestUtils.createSimpleMsbContext();
        msbContextSpy = spy(msbContext);
        mockChannelManager = mock(ChannelManager.class);
        mockProducer = mock(Producer.class);
        emptyPayload = new Payload.Builder().build();
        originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);

        when(msbContextSpy.getChannelManager()).thenReturn(mockChannelManager);
        when(msbContextSpy.getMessageFactory()).thenReturn(spyMessageFactory);
        when(mockChannelManager.findOrCreateProducer(anyString())).thenReturn(mockProducer);

        responder = new ResponderImpl(messageTemplate, originalMessage, msbContextSpy);
    }

    @Test
    public void testResponderConstructorOk() {
        MsbContextImpl context = TestUtils.createSimpleMsbContext();
        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        new ResponderImpl(messageTemplate, originalMessage, context);
    }

    @Test
    public void testProducerWasCreatedForProperTopic() {
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        responder.send(emptyPayload);

        verify(mockChannelManager).findOrCreateProducer(argument.capture());

        assertEquals(originalMessage.getTopics().getResponse(), argument.getValue());
    }

    @Test
    public void testProducerPublishMethodInvoked() {
        responder.send(emptyPayload);

        verify(mockProducer, times(1)).publish(anyObject());
    }

    @Test
    public void testProducerPublishUseCorrectPayload() {
        String bodyText = "This is body";
        Payload<?, ?, ?, String> simplePayload = TestUtils.createPayloadWithTextBody(bodyText);

        ArgumentCaptor<Message> argument = ArgumentCaptor.forClass(Message.class);
        responder.send(simplePayload);

        verify(mockProducer).publish(argument.capture());

        assertNotNull(argument.getValue().getRawPayload());
        TestUtils.assertRawPayloadContainsBodyText(bodyText, argument.getValue());
    }

    @Test
    public void testProducerPublishWithTags() {
        String[] tags = new String[]{"tag1", "tag2"};
        responder = new ResponderImpl(TestUtils.createSimpleMessageTemplate(tags), originalMessage, msbContextSpy);

        ArgumentCaptor<Message> argument = ArgumentCaptor.forClass(Message.class);
        responder.send(TestUtils.createSimpleRequestPayload());

        verify(mockProducer).publish(argument.capture());

        Message responseMessage = argument.getValue();
        assertArrayEquals(tags, responseMessage.getTags().toArray());
    }

    @Test
    public void testAckBuilderContainsCorrectProperties() {
        Integer timeout = 222;
        Integer responsesRemaining = 2;
        ArgumentCaptor<Message> argument = ArgumentCaptor.forClass(Message.class);
        responder.sendAck(timeout, responsesRemaining);

        verify(mockProducer).publish(argument.capture());

        assertEquals(argument.getValue().getAck().getTimeoutMs(), timeout);
        assertEquals(argument.getValue().getAck().getResponsesRemaining(), responsesRemaining);
    }

    @Test
    public void testSendWithSameResponderId() {
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        responder.sendAck(1000, 1);

        verify(mockProducer).publish(messageCaptor.capture());
        String responderIdInAck = messageCaptor.getValue().getAck().getResponderId();

        reset(mockProducer);
        responder.send(TestUtils.createSimpleResponsePayload());
        verify(mockProducer).publish(messageCaptor.capture());

        String responderIdInAckPayload = messageCaptor.getValue().getAck().getResponderId();
        assertEquals(responderIdInAck, responderIdInAckPayload);
    }
}
