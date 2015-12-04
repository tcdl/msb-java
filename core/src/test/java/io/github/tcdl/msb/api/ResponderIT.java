package io.github.tcdl.msb.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.github.tcdl.msb.adapters.mock.MockAdapter;
import io.github.tcdl.msb.api.exception.JsonSchemaValidationException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.impl.ResponderImpl;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ResponderIT {

    private static final Logger LOG = LoggerFactory.getLogger(ResponderIT.class);

    private final static String STATIC_TAG = "responder-tag";

    private MsbContextImpl msbContext;
    private JsonValidator validator;
    private ObjectMapper payloadMapper;

    @Before
    public void setUp() throws Exception {
        msbContext = (MsbContextImpl) new MsbContextBuilder().build();
        validator = new JsonValidator();
        payloadMapper = msbContext.getPayloadMapper();
    }

    @Test
    public void testCreateAckMessage() throws Exception {
        String namespace = "test:responder-ack";
        int ackTimeout = 1000;
        int responsesRemaining = 2;
        MessageTemplate messageOptions = TestUtils.createSimpleMessageTemplate();
        Message originalMessage = TestUtils.createSimpleRequestMessage(namespace);

        Responder responder = new ResponderImpl(messageOptions, originalMessage, null, msbContext);

        responder.sendAck(ackTimeout, responsesRemaining);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(originalMessage.getTopics().getResponse());

        assertNotNull("Ack message shouldn't be null", adapterJsonMessage);
        assertAckMessage(adapterJsonMessage, ackTimeout, responsesRemaining, originalMessage.getTopics().getResponse());
    }

    private void assertAckMessage(String receivedAckMsg, int ackTimeout, int responsesRemaining, String responseNamespace) {
        try {
            validator.validate(receivedAckMsg, this.msbContext.getMsbConfig().getSchema());

            JsonNode jsonObject = payloadMapper.readTree(receivedAckMsg);

            // ack fields set
            assertTrue("Message not contain expected property ack", jsonObject.has("ack"));
            assertTrue("Message not contain expected property ack.responderId", jsonObject.get("ack").has("responderId"));
            assertTrue("Message not contain expected property ack.responsesRemaining", jsonObject.get("ack").has("responsesRemaining"));
            assertTrue("Message not contain expected property ack.timeoutMs", jsonObject.get("ack").has("timeoutMs"));

            // ack fields match sent
            assertEquals("Message 'ack.responsesRemaining' is incorrect", responsesRemaining, jsonObject.get("ack").get("responsesRemaining").asInt());
            assertEquals("Message 'ack.timeoutMs' is incorrect", ackTimeout, jsonObject.get("ack").get("timeoutMs").asInt());

            // topics
            TestUtils.assertJsonContains(jsonObject.get("topics"), "to", responseNamespace);
            assertFalse(jsonObject.get("topics").has("response"));
        } catch (JsonSchemaValidationException | IOException e) {
            LOG.error("Exception while parse message ack", e);
            fail("Message validation failed");
        }
    }

    @Test
    public void testCreateResponseMessage() throws Exception {
        String namespace = "test:responder-response";
        MessageTemplate messageOptions = TestUtils.createSimpleMessageTemplate();
        Message originalMessage = TestUtils.createSimpleRequestMessage(namespace);

        Responder responder = new ResponderImpl(messageOptions, originalMessage, null, msbContext);
        RestPayload responsePayload = TestUtils.createSimpleResponsePayload();
        responder.send(responsePayload);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(originalMessage.getTopics().getResponse());

        assertNotNull("Response message shouldn't be null", adapterJsonMessage);
        TestUtils.assertResponseMessagePayload(adapterJsonMessage, responsePayload, originalMessage.getTopics().getResponse());
    }

    @Test
    public void testCreateResponseMessageWithTags() throws Exception {
        String namespace = "test:responder-response";
        MessageTemplate messageOptions = TestUtils.createSimpleMessageTemplate(STATIC_TAG);
        String dynamicTagOriginal = "dynamic-tag-original";
        Message originalMessage = TestUtils.createSimpleRequestMessageWithTags(namespace, dynamicTagOriginal);

        Responder responder = new ResponderImpl(messageOptions, originalMessage, null, msbContext);
        RestPayload responsePayload = TestUtils.createSimpleResponsePayload();
        responder.send(responsePayload);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(originalMessage.getTopics().getResponse());

        assertNotNull("Response message shouldn't be null", adapterJsonMessage);
        TestUtils.assertResponseMessagePayload(adapterJsonMessage, responsePayload, originalMessage.getTopics().getResponse());
        TestUtils.assertMessageTags(adapterJsonMessage, dynamicTagOriginal, STATIC_TAG);
    }
}
