package io.github.tcdl.messages;

import io.github.tcdl.config.MessageTemplate;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.ServiceDetails;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class MessageFactoryTest {

    private final Instant FIXED_CLOCK_INSTANT = Instant.parse("2007-12-03T10:15:30.00Z");
    private final Clock FIXED_CLOCK = Clock.fixed(FIXED_CLOCK_INSTANT, ZoneId.systemDefault());

    @Mock
    private MessageTemplate messageOptions;

    @Mock
    private MsbConfigurations msbConf;

    private ServiceDetails serviceDetails = TestUtils.createMsbConfigurations().getServiceDetails();

    private MessageFactory messageFactory = new MessageFactory(serviceDetails, FIXED_CLOCK);

    @Test
    public void testCreateRequestMessageHasBasicFieldsSet() {
        String namespace = "test:request-basic-fields";
        Message originalMessage = TestUtils.createMsbResponseMessage(namespace);
        MessageBuilder requestMesageBuilder = messageFactory.createRequestMessageBuilder(namespace, messageOptions, originalMessage);

        Message message = requestMesageBuilder.build();

        assertThat(message.getCorrelationId(), is(originalMessage.getCorrelationId()));
        assertThat(message.getTopics().getTo(), is(namespace));
        assertThat(message.getTopics().getResponse(), notNullValue());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateResponseMessageNullOriginalMessage() {
        messageFactory.createResponseMessage(null, null, null);
    }

    @Test
    public void testCreateResponseMessageHasBasicFieldsSet() {
        String namespace = "test:request-basic-fields";
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload(namespace);
        MessageBuilder responseMesageBuilder = messageFactory.createResponseMessageBuilder(messageOptions, originalMessage);

        Message message = responseMesageBuilder.build();

        assertThat(message.getCorrelationId(), is(originalMessage.getCorrelationId()));
        assertThat(message.getTopics().getTo(), not(namespace));
        assertThat(message.getTopics().getTo(), not(originalMessage.getTopics().getTo()));
        assertThat(message.getTopics().getTo(), is(originalMessage.getTopics().getResponse()));
    }

    @Test
    public void testCreateRequestMessageWithPayload() {
        Payload requestPayload = TestUtils.createSimpleResponsePayload();
        MessageBuilder requestMesageBuilder = TestUtils.createMesageBuilder();

        Message message = messageFactory.createRequestMessage(requestMesageBuilder, requestPayload);

        assertEquals("Message payload is not set correctly", requestPayload, message.getPayload());
        assertNull(message.getAck());
    }

    @Test
    public void testCreateRequestMessageWithoutPayload() {
        MessageBuilder requestMesageBuilder = TestUtils.createMesageBuilder();

        Message message = messageFactory.createRequestMessage(requestMesageBuilder, null);

        assertNull(message.getPayload());
        assertNull(message.getAck());
    }

    @Test
    public void testCreateResponseMessageWithPayloadAndAck() {
        MessageBuilder responseMesageBuilder = TestUtils.createMesageBuilder();
        Payload responsePayload = TestUtils.createSimpleResponsePayload();
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().withResponderId(Utils.generateId()).withResponsesRemaining(3).withTimeoutMs(100).build();

        Message message = messageFactory.createResponseMessage(responseMesageBuilder, ack, responsePayload);

        assertEquals("Message payload is not set correctly", responsePayload, message.getPayload());
        assertEquals("Message ack is not set correctly", ack, message.getAck());
    }

    @Test
    public void testCreateResponseMessageWithoutPayloadAndAck() {
        MessageBuilder responseMesageBuilder = TestUtils.createMesageBuilder();

        Message message = messageFactory.createResponseMessage(responseMesageBuilder, null, null);

        assertNull("Message payload is not expected", message.getPayload());
        assertNull("Message ack is not expected", message.getAck());
    }

    @Test
    public void testCreateAckBuilder() throws Exception {
        Acknowledge ack = messageFactory.createAckBuilder().build();
        assertNotNull("ack responderId not set", ack.getResponderId());
    }

    @Test
    public void testBroadcastMessage() {
        String topic = "topic:target";

        Payload broadcastPayload = TestUtils.createSimpleBroadcastPayload();
        MessageBuilder messageBuilder = messageFactory.createBroadcastMessageBuilder(messageOptions, topic, broadcastPayload);

        Message message = messageBuilder.build();

        assertEquals(topic, message.getTopics().getTo());
        assertNull(message.getTopics().getResponse());
        assertEquals(broadcastPayload, message.getPayload());
    }
}
