package io.github.tcdl.msb.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.mock.MockAdapter;
import io.github.tcdl.msb.api.exception.JsonSchemaValidationException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.impl.ResponderImpl;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResponderIT {

    private static final Logger LOG = LoggerFactory.getLogger(ResponderIT.class);

    private MsbContextImpl msbContext;
    private JsonValidator validator;
    private ObjectMapper messageMapper;

    @Before
    public void setUp() throws Exception {
        msbContext = (MsbContextImpl) new MsbContextBuilder().build();
        validator = new JsonValidator();
        this.messageMapper = msbContext.getPayloadMapper();
    }

    @Test
    public void testCreateAckMessage() throws Exception {
        String namespace = "test:responder-ack";
        int ackTimeout = 1000;
        int responsesRemaining = 2;
        MessageTemplate messageOptions = TestUtils.createSimpleMessageTemplate();
        Message originalMessage = TestUtils.createMsbRequestMessageWithSimplePayload(namespace);

        Responder responder = new ResponderImpl(messageOptions, originalMessage, msbContext);

        responder.sendAck(ackTimeout, responsesRemaining);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(originalMessage.getTopics().getResponse());

        assertNotNull("Ack message shouldn't be null", adapterJsonMessage);
        assertAckMessage(adapterJsonMessage, ackTimeout, responsesRemaining, originalMessage.getTopics().getResponse());
    }

    private void assertAckMessage(String receivedAckMsg, int ackTimeout, int responsesRemaining, String responseNamespace) {
        try {
            validator.validate(receivedAckMsg, this.msbContext.getMsbConfig().getSchema());

            JSONObject jsonObject = new JSONObject(receivedAckMsg);
            
            // ack fields set 
            assertTrue("Message not contain expected property ack", jsonObject.has("ack"));
            assertTrue("Message not contain expected property ack.responderId", jsonObject.getJSONObject("ack").has("responderId"));
            assertTrue("Message not contain expected property ack.responsesRemaining", jsonObject.getJSONObject("ack").has("responsesRemaining"));
            assertTrue("Message not contain expected property ack.timeoutMs", jsonObject.getJSONObject("ack").has("timeoutMs"));

            // ack fields match sent
            assertEquals("Message 'ack.responsesRemaining' is incorrect", responsesRemaining, jsonObject
                    .getJSONObject("ack").get("responsesRemaining"));
            assertEquals("Message 'ack.timeoutMs' is incorrect", ackTimeout, jsonObject
                    .getJSONObject("ack").get("timeoutMs"));

            //topics
            assertJsonContains(jsonObject.getJSONObject("topics"), "to", responseNamespace);
            assertTrue(jsonObject.getJSONObject("topics").isNull("response"));
        } catch (JsonSchemaValidationException | JSONException e) {
            LOG.error("Exception while parse message ack", e);
            fail("Message validation failed");
        }
    }

    @Test
    public void testCreateResponseMessage() throws Exception {
        String namespace = "test:responder-response";
        MessageTemplate messageOptions = TestUtils.createSimpleMessageTemplate();
        Message originalMessage = TestUtils.createMsbRequestMessageWithSimplePayload(namespace);

        Responder responder = new ResponderImpl(messageOptions, originalMessage, msbContext);
        Payload responsePayload = TestUtils.createSimpleResponsePayload();
        responder.send(responsePayload);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(originalMessage.getTopics().getResponse());

        assertNotNull("Response message shouldn't be null", adapterJsonMessage);
        assertResponsePayload(adapterJsonMessage, responsePayload, originalMessage.getTopics().getResponse());
    }

    private void assertResponsePayload(String receivedResponseMsg, Payload originalResponsePayload, String responseNamespace) {
        try {
            validator.validate(receivedResponseMsg, this.msbContext.getMsbConfig().getSchema());
            JSONObject jsonObject = new JSONObject(receivedResponseMsg);

            // payload fields set 
            assertTrue("Message not contain 'body' filed", jsonObject.getJSONObject("payload").has("body"));
            assertTrue("Message not contain 'headers' filed", jsonObject.getJSONObject("payload").has("headers"));

            // payload fields match sent
            assertEquals("Message 'body' is incorrect", messageMapper.writeValueAsString(originalResponsePayload.getBody()),
                    jsonObject.getJSONObject("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", messageMapper.writeValueAsString(originalResponsePayload.getHeaders()),
                    jsonObject.getJSONObject("payload").get("headers").toString());

            //topics
            assertJsonContains(jsonObject.getJSONObject("topics"), "to", responseNamespace);
            assertTrue(jsonObject.getJSONObject("topics").isNull("response"));

        } catch (JsonSchemaValidationException | JSONException | JsonProcessingException e) {
            LOG.error("Exception while parse message payload", e);
            fail("Message validation failed");
        }
    }

    private void assertJsonContains(JSONObject jsonObject, String field, Object value) {
        assertTrue(jsonObject.has(field));
        assertNotNull(jsonObject.get(field));
        assertEquals(value, jsonObject.get(field));
    }
}
