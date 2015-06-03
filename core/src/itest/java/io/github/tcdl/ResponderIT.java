package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.github.tcdl.adapters.mock.MockAdapter;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ResponderIT {

    public static final Logger LOG = LoggerFactory.getLogger(ResponderIT.class);

    private MsbContext msbContext;

    @Before
    public void setUp() throws Exception {
        msbContext = new MsbContext.MsbContextBuilder().build();
    }

    @Test
    public void testCreateAckMessage() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:responder-ack");
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(messageOptions.getNamespace());

        Responder responder = new Responder(messageOptions, originalMessage, msbContext);

        responder.sendAck(1000, 2);
        Message ackMessage = responder.getResponseMessage();

        assertNull("Message payload shouldn't be set", ackMessage.getPayload());

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(originalMessage.getTopics().getResponse());
        assertAckMessage(adapterJsonMessage, ackMessage);
    }

    private void assertAckMessage(String recievedAckMsg, Message originalAckMsg) {
        try {
            Utils.validateJsonWithSchema(recievedAckMsg, this.msbContext.getMsbConfig().getSchema());

            JSONObject jsonObject = new JSONObject(recievedAckMsg);
            
            // ack fields set 
            assertTrue("Message not contain expected property ack", jsonObject.has("ack"));
            assertTrue("Message not contain expected property ack.responderId", jsonObject.getJSONObject("ack").has("responderId"));
            assertTrue("Message not contain expected property ack.responsesRemaining", jsonObject.getJSONObject("ack").has("responsesRemaining"));
            assertTrue("Message not contain expected property ack.timeoutMs", jsonObject.getJSONObject("ack").has("timeoutMs"));

            // ack fields match sended 
            assertEquals("Message 'ack.responderId' is incorrect", originalAckMsg.getAck().getResponderId(), jsonObject
                    .getJSONObject("ack").getString("responderId"));
            assertEquals("Message 'ack.responsesRemaining' is incorrect", originalAckMsg.getAck().getResponsesRemaining(), jsonObject
                    .getJSONObject("ack").get("responsesRemaining"));
            assertEquals("Message 'ack.timeoutMs' is incorrect", originalAckMsg.getAck().getTimeoutMs(), jsonObject
                    .getJSONObject("ack").get("timeoutMs"));

            //topics
            assertJsonContains(jsonObject.getJSONObject("topics"), "to", originalAckMsg.getTopics().getTo());
            assertTrue(jsonObject.getJSONObject("topics").isNull("response"));
        } catch (JsonSchemaValidationException | JSONException e) {
            LOG.error("Exception while parse message ack", e);
            fail("Message validation failed");
        }
    }

    @Test
    public void testCreateResponseMessage() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:responder-response");
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(messageOptions.getNamespace());

        Responder responder = new Responder(messageOptions, originalMessage, msbContext);
        Payload responsePayload = TestUtils.createSimpleResponsePayload();
        responder.send(responsePayload);
        Message responseMessage = responder.getResponseMessage();

        assertEquals("Message payload not match sended", responsePayload, responseMessage.getPayload());

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(originalMessage.getTopics().getResponse());
        assertResponseMessage(adapterJsonMessage, responseMessage);
    }

    private void assertResponseMessage(String recievedResponseMsg, Message originalResponseMsg) {
        try {
            Utils.validateJsonWithSchema(recievedResponseMsg, this.msbContext.getMsbConfig().getSchema());
            JSONObject jsonObject = new JSONObject(recievedResponseMsg);

            // payload fields set 
            assertTrue("Message not contain 'body' filed", jsonObject.getJSONObject("payload").has("body"));
            assertTrue("Message not contain 'headers' filed", jsonObject.getJSONObject("payload").has("headers"));

            // payload fields match sended 
            assertEquals("Message 'body' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(originalResponseMsg.getPayload().getBody()),
                    jsonObject.getJSONObject("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(originalResponseMsg.getPayload().getHeaders()),
                    jsonObject
                    .getJSONObject("payload").get("headers").toString());

            //topics
            assertJsonContains(jsonObject.getJSONObject("topics"), "to", originalResponseMsg.getTopics().getTo());
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
