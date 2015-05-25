package io.github.tcdl.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by rdro on 4/23/2015.
 */
public class MessageFactoryTest {

    private MsbMessageOptions messageOptions;
    private MsbConfigurations msbConf;
    private MessageFactory messageFactory;

    @Before
    public void setUp() throws Exception {
        this.messageOptions = TestUtils.createSimpleConfig();
        this.msbConf = TestUtils.createMsbConfigurations();
        this.messageFactory = new MessageFactory(msbConf.getServiceDetails());
    }

    @Test
    public void testCreateRequestMessageHasBasicFieldsSet() {
        Message originalMessage = TestUtils.createMsbResponseMessage();
        MessageBuilder requestMesageBuilder = messageFactory.createRequestMessage(messageOptions, originalMessage);
        Message message = requestMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();

        assertEquals("Message correlationId must match originalMessage", originalMessage.getCorrelationId(), message.getCorrelationId());
        assertNotNull("Message topic response is not set", message.getTopics().getResponse());
        assertEquals("Message topic to is not correct", messageOptions.getNamespace(), message.getTopics().getTo());
    }

    @Test
    public void testCreateResponseMessageHasBasicFieldsSet() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        MessageBuilder responseMesageBuilder = messageFactory.createResponseMessage(originalMessage, null, null);
        Message message = responseMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();

        assertEquals("Message correlationId must match originalMessage", originalMessage.getCorrelationId(), message.getCorrelationId());
        assertNull("Message topic response shouldn't be set", message.getTopics().getResponse());
        assertNotEquals("Message topic to equals to original message topic to", originalMessage.getTopics().getTo(), message.getTopics().getTo());
        assertEquals("Message topic to is not correct", originalMessage.getTopics().getResponse(), message.getTopics().getTo());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateResponseMessageNullOriginalThrowsException() {
        messageFactory.createResponseMessage(null, null, null);
    }

    @Test
    public void testCreateResponseMessageWithAck() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(3).setTimeoutMs(100).build();
        MessageBuilder responseMesageBuilder = messageFactory.createResponseMessage(originalMessage, ack, null);
        Message message = responseMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();

        assertEquals("Message ack is not set correctly", ack, message.getAck());
    }

    @Test
    public void testCreateResponseMessageWithoutAck() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        MessageBuilder responseMesageBuilder = messageFactory.createResponseMessage(originalMessage, null, null);
        Message message = responseMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();
        assertNull("Message ack shouldn't be set", message.getAck());
    }

    @Test
    public void testCreateResponseMessageWithPayload() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        Payload responsePayload = TestUtils.createSimpleResponsePayload();
        MessageBuilder responseMesageBuilder = messageFactory.createResponseMessage(originalMessage, null, responsePayload);
        Message message = responseMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();
        assertEquals("Message payload is not set correctly", responsePayload, message.getPayload());
    }

    @Test
    public void testCreateResponseMessageWithoutPayload() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        MessageBuilder responseMesageBuilder = messageFactory.createResponseMessage(originalMessage, null, null);
        Message message = responseMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();
        assertNull("Message payload shouldn't be set", message.getPayload());
    }

    @Test
    public void testCreateAckMessageHasBasicFieldsSet() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        MessageBuilder ackMesageBuilder = messageFactory.createAckMessage(originalMessage, null);
        Message message = ackMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();

        assertEquals("Message correlationId must match originalMessage", originalMessage.getCorrelationId(), message.getCorrelationId());
        assertNull("Message topic response shouldn't be set", message.getTopics().getResponse());
        assertNotEquals("Message topic to equals to original message topic to", originalMessage.getTopics().getTo(), message.getTopics().getTo());
        assertEquals("Message topic to is not correct", originalMessage.getTopics().getResponse(), message.getTopics().getTo());
    }
    
    @Test(expected = NullPointerException.class)
    public void testCreateAckMessageNullOriginalThrowsException() {
        messageFactory.createAckMessage(null, null);
    }


    @Test
    public void testCreateAckMessageWithAck() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(2).setTimeoutMs(200).build();
        MessageBuilder ackMesageBuilder = messageFactory.createAckMessage(originalMessage, ack);
        Message message = ackMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();

        assertEquals("Message ack is not set correctly", ack, message.getAck());
        assertNull("Message payload shouldn't be set", message.getPayload());
    }

    @Test
    public void testCreateAckMessageWithoutAck() {
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload();
        MessageBuilder ackMesageBuilder = messageFactory.createAckMessage(originalMessage, null);
        Message message = ackMesageBuilder.setMeta(TestUtils.createSimpleMeta(msbConf)).build();

        assertNull("Message ack shouldn't be set", message.getAck());
        assertNull("Message payload shouldn't be set", message.getPayload());
    }

    @Test
    public void testCreateAck() throws Exception {
        Acknowledge message = messageFactory.createAck().build();
        assertNotNull("ack responderId not set", message.getResponderId());
    }

    @Test
    public void testCreateMeta() throws Exception {
        MessageBuilder messageBuilder = new MessageBuilder().setId(Utils.generateId()).setCorrelationId(Utils.generateId())
                .setTopics(new Topics.TopicsBuilder().setTo("to").build());
        MetaMessageBuilder metaBuilder = messageFactory.createMeta(messageOptions);
        Message message = messageFactory.completeMeta(
                messageBuilder, metaBuilder);

        assertEquals("Message ttl is not set correctly", messageOptions.getTtl(), message.getMeta().getTtl());
        assertEquals("Message create_at is not equals now", DateUtils.truncate(new Date(), Calendar.SECOND),
                DateUtils.truncate(message.getMeta().getCreatedAt(), Calendar.SECOND));

        assertNotNull("Message ack is not set", message.getMeta().getServiceDetails());
        assertEquals("Message ack is not set correctly", message.getMeta().getServiceDetails(), msbConf.getServiceDetails());
    }
    @Test
    public void testCreateMetaNullConf() throws Exception {
        MessageBuilder messageBuilder = new MessageBuilder().setId(Utils.generateId()).setCorrelationId(Utils.generateId())
                .setTopics(new Topics.TopicsBuilder().setTo("to").build());
        MetaMessageBuilder metaBuilder = messageFactory.createMeta(null);
        Message message = messageFactory.completeMeta(
                messageBuilder, metaBuilder);

        assertNull("Message should be null", message.getMeta().getTtl());
        assertEquals("Message create_at is not equals now", DateUtils.truncate(new Date(), Calendar.SECOND),
                DateUtils.truncate(message.getMeta().getCreatedAt(), Calendar.SECOND));

        assertNotNull("Message ack is not set", message.getMeta().getServiceDetails());
        assertEquals("Message ack is not set correctly", message.getMeta().getServiceDetails(), msbConf.getServiceDetails());
    }

    @Test
    public void testCompleteMeta() throws Exception {
        int delay = 10000;
        MetaMessageBuilder metaBuilder = messageFactory.createMeta(messageOptions);
        MessageBuilder messageBuilder = messageFactory.createRequestMessage(messageOptions, null);
        Message message = messageFactory.completeMeta(messageBuilder, metaBuilder);

        assertTrue(message.getMeta().getDurationMs().intValue() - delay < 10);
    }
}
