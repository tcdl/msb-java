package io.github.tcdl.messages;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.config.ServiceDetails;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessageFactoryTest {

    @Mock
    private MsbMessageOptions messageOptions;

    @Mock
    private MsbConfigurations msbConf;

    private ServiceDetails serviceDetails = TestUtils.createMsbConfigurations().getServiceDetails();

    private MessageFactory messageFactory = new MessageFactory(serviceDetails);

    @Test
    public void testCreateRequestMessageHasBasicFieldsSet() {
        when(messageOptions.getNamespace()).thenReturn("test:request-basic-fields");
        Message originalMessage = TestUtils.createMsbResponseMessage();
        MessageBuilder requestMesageBuilder = messageFactory.createRequestMessageBuilder(messageOptions, originalMessage);

        Message message = requestMesageBuilder.build();

        assertThat(message.getCorrelationId(), is(originalMessage.getCorrelationId()));
        assertThat(message.getTopics().getTo(), is(messageOptions.getNamespace()));
        assertThat(message.getTopics().getResponse(), notNullValue());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateResponseMessageNullOriginalMessage() {
        messageFactory.createResponseMessage(null, null, null);
    }

    @Test
    public void testCreateResponseMessageHasBasicFieldsSet() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        MessageBuilder responseMesageBuilder = messageFactory.createResponseMessageBuilder(messageOptions, originalMessage);

        Message message = responseMesageBuilder.build();

        assertThat(message.getCorrelationId(), is(originalMessage.getCorrelationId()));
        assertThat(message.getTopics().getTo(), not(messageOptions.getNamespace()));
        assertThat(message.getTopics().getTo(), not(originalMessage.getTopics().getTo()));
        assertThat(message.getTopics().getTo(), is(originalMessage.getTopics().getResponse()));
    }

    @Test
    public void testCreateRequestMessageWithPayload() {
        when(messageOptions.getNamespace()).thenReturn("test:request-with-payload");
        Payload requestPayload = TestUtils.createSimpleResponsePayload();
        MessageBuilder requestMesageBuilder = TestUtils.createMesageBuilder();

        Message message = messageFactory.createRequestMessage(requestMesageBuilder, requestPayload);

        assertEquals("Message payload is not set correctly", requestPayload, message.getPayload());
        assertNull(message.getAck());
    }

    @Test
    public void testCreateRequestMessageWithoutPayload() {
        when(messageOptions.getNamespace()).thenReturn("test:request-without-payload");
        MessageBuilder requestMesageBuilder = TestUtils.createMesageBuilder();

        Message message = messageFactory.createRequestMessage(requestMesageBuilder, null);

        assertNull(message.getPayload());
        assertNull(message.getAck());
    }

    @Test
    public void testCreateResponseMessageWithPayloadAndAck() {
        MessageBuilder responseMesageBuilder = TestUtils.createMesageBuilder();
        Payload responsePayload = TestUtils.createSimpleResponsePayload();
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(3).setTimeoutMs(100).build();

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
    public void testCreateMessageBuilderMetaFromMsgOptions() throws Exception {
        Integer ttl = Integer.valueOf(123);
        when(messageOptions.getNamespace()).thenReturn("test:meta-set");
        when(messageOptions.getTtl()).thenReturn(ttl);
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        MessageBuilder mesageBuilder = messageFactory.createResponseMessageBuilder(messageOptions, originalMessage);

        Message message = mesageBuilder.build();
        assertEquals("Message ttl is not set correctly", ttl, message.getMeta().getTtl());
        assertEquals("Message create_at is not equals now", DateUtils.truncate(new Date(), Calendar.SECOND),
                DateUtils.truncate(message.getMeta().getCreatedAt(), Calendar.SECOND));

        assertThat(message.getMeta().getDurationMs().intValue(), equalTo(0));
    }

    @Test
    public void testDurationMsIsSet() throws Exception {
        when(messageOptions.getNamespace()).thenReturn("test:durationMs");
        MessageBuilder requestMesageBuilder = TestUtils.createMesageBuilder();
        Payload requestPayload = TestUtils.createSimpleResponsePayload();

        Message message = messageFactory.createRequestMessage(requestMesageBuilder, requestPayload);

        assertThat(message.getMeta().getDurationMs().intValue(), not(0));
    }

    @Test
    public void testBroadcastMessage() {
        String topic = "topic:target";

        Payload broadcastPayload = TestUtils.createSimpleBroadcastPayload();
        MessageBuilder messageBuilder = messageFactory.createBroadcastMessage(messageOptions, topic, broadcastPayload);

        Message message = messageBuilder.build();

        assertEquals(topic, message.getTopics().getTo());
        assertNull(message.getTopics().getResponse());
        assertEquals(broadcastPayload, message.getPayload());
    }
}
