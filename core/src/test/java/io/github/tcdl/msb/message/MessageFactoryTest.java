package io.github.tcdl.msb.message;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.Message.Builder;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
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
    private MsbConfig msbConf;

    private ServiceDetails serviceDetails = TestUtils.createMsbConfigurations().getServiceDetails();

    private MessageFactory messageFactory = new MessageFactory(serviceDetails, FIXED_CLOCK, TestUtils.createMessageMapper());

    @Test
    public void testCreateRequestMessageHasBasicFieldsSet() {
        String namespace = "test:request-basic-fields";
        Message originalMessage = TestUtils.createMsbResponseMessage(namespace);
        Builder requestMesageBuilder = messageFactory.createRequestMessageBuilder(namespace, messageOptions, originalMessage);

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
        Builder responseMessageBuilder = messageFactory.createResponseMessageBuilder(messageOptions, originalMessage);

        Message message = responseMessageBuilder.build();

        assertThat(message.getCorrelationId(), is(originalMessage.getCorrelationId()));
        assertThat(message.getTopics().getTo(), not(namespace));
        assertThat(message.getTopics().getTo(), not(originalMessage.getTopics().getTo()));
        assertThat(message.getTopics().getTo(), is(originalMessage.getTopics().getResponse()));
    }

    @Test
    public void testCreateRequestMessageWithPayload() {
        String bodyText = "body text";
        Payload requestPayload = TestUtils.createPayloadWithTextBody(bodyText);
        Builder requestMessageBuilder = TestUtils.createMessageBuilder();

        Message message = messageFactory.createRequestMessage(requestMessageBuilder, requestPayload);

        TestUtils.assertRawPayloadContainsBodyText(bodyText, message);
        assertNull(message.getAck());
    }

    @Test
    public void testCreateRequestMessageWithoutPayload() {
        Builder requestMessageBuilder = TestUtils.createMessageBuilder();

        Message message = messageFactory.createRequestMessage(requestMessageBuilder, null);

        assertNull(message.getRawPayload());
        assertNull(message.getAck());
    }

    @Test
    public void testCreateResponseMessageWithPayloadAndAck() {
        String bodyText = "body text";
        Builder responseMessageBuilder = TestUtils.createMessageBuilder();
        Payload responsePayload = TestUtils.createPayloadWithTextBody(bodyText);
        Acknowledge ack = new Acknowledge.Builder()
                .withResponderId(Utils.generateId())
                .withResponsesRemaining(3)
                .withTimeoutMs(100)
                .build();

        Message message = messageFactory.createResponseMessage(responseMessageBuilder, ack, responsePayload);

        TestUtils.assertRawPayloadContainsBodyText(bodyText, message);
        assertEquals("Message ack is not set correctly", ack, message.getAck());
    }

    @Test
    public void testCreateResponseMessageWithoutPayloadAndAck() {
        Builder responseMessageBuilder = TestUtils.createMessageBuilder();

        Message message = messageFactory.createResponseMessage(responseMessageBuilder, null, null);

        assertNull("Message payload is not expected", message.getRawPayload());
        assertNull("Message ack is not expected", message.getAck());
    }

    @Test
    public void testCreateAckBuilder() throws Exception {
        Acknowledge ack = messageFactory.createAckBuilder().build();
        assertNotNull("ack responderId not set", ack.getResponderId());
    }

    @Test
    public void testBroadcastMessage() {
        String bodyText = "body text";
        String topic = "topic:target";

        Payload broadcastPayload = TestUtils.createPayloadWithTextBody(bodyText);
        Builder messageBuilder = messageFactory.createBroadcastMessageBuilder(messageOptions, topic, broadcastPayload);

        Message message = messageBuilder.build();

        assertEquals(topic, message.getTopics().getTo());
        assertNull(message.getTopics().getResponse());
        TestUtils.assertRawPayloadContainsBodyText(bodyText, message);
    }
}
