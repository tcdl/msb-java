package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.github.tcdl.adapters.Adapter.RawMessageHandler;
import io.github.tcdl.adapters.MockAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ResponderIT {

    public static final Logger LOG = LoggerFactory.getLogger(ResponderIT.class);

    private MsbMessageOptions messageOptions;
    private MsbConfigurations msbConf;

    @Before
    public void setUp() throws Exception {
        this.messageOptions = TestUtils.createSimpleConfig();
        this.msbConf = MsbConfigurations.msbConfiguration();
        MockAdapter.getInstance().clearAllMessages();
    }

    @Test    
    public void testCreateAckMessage() throws Exception {
        Responder responder = new Responder(messageOptions, TestUtils.createMsbRequestMessageNoPayload());
        responder.sendAck(1000, 2, null);
        Message message = responder.getResponseMessage();

        assertNull("Message payload shouldn't be set", message.getPayload());

        MockAdapter.getInstance().subscribe(new RawMessageHandler() {

            @Override
            public void onMessage(String jsonMessage) {
                assertAckMessage(jsonMessage);

            }
        });
    }

    private void assertAckMessage(String json) {
        try {
            assertTrue("Message didn't correspondent to expected schema",
                    Utils.validateJsonWithSchema(json, this.msbConf.getSchema()));
        } catch (JsonSchemaValidationException e) {
            fail("Message schema validation failed");
        }
        JSONObject jsonObject = new JSONObject(json);

        // ack  
        assertTrue("Message not contain expected property ack", jsonObject.has("ack"));
        //assertTrue("Message not contain expected property ack.responderId", jsonObject.getJSONObject("ack").has("responderId"));
        //assertTrue("Message not contain expected property ack.responsesRemaining", jsonObject.getJSONObject("ack").has("responsesRemaining"));
        //assertTrue("Message not contain expected property ack.timeoutMs", jsonObject.getJSONObject("ack").has("timeoutMs"));

        //topics
        assertJsonContains(jsonObject.getJSONObject("topics"), "to", messageOptions.getNamespace() + ":response:"
                + msbConf.getServiceDetails().getInstanceId());
        assertTrue(jsonObject.getJSONObject("topics").isNull("response"));
    }

    @Test
    public void testCreateResponseMessage() throws Exception {
        Responder responder = new Responder(messageOptions, TestUtils.createMsbRequestMessageNoPayload());
        Payload responsePayload = TestUtils.createSimpleResponsePayload();
        responder.send(responsePayload, null);
        Message message = responder.getResponseMessage();

        assertEquals("Message payload not match sended", responsePayload, message.getPayload());

        MockAdapter.getInstance().subscribe(new RawMessageHandler() {

            @Override
            public void onMessage(String jsonMessage) {
                assertResponseMessage(jsonMessage, responsePayload);
            }
        });
    }

    private void assertResponseMessage(String json, Payload responsePayload) {
        try {
            assertTrue("Message didn't correspondent to expected schema",
                    Utils.validateJsonWithSchema(json, this.msbConf.getSchema()));
            JSONObject jsonObject = new JSONObject(json);

            // payload
            assertTrue("Message not contain 'body' filed", jsonObject.getJSONObject("payload").has("body"));
            assertEquals("Message 'body' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(responsePayload.getBody()),
                    jsonObject.getJSONObject("payload").get("body").toString());

            assertTrue("Message not contain 'headers' filed", jsonObject.getJSONObject("payload").has("headers"));
            assertEquals("Message 'headers' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(responsePayload.getHeaders()), jsonObject
                    .getJSONObject("payload").get("headers").toString());

            //topics
            assertJsonContains(jsonObject.getJSONObject("topics"), "to", messageOptions.getNamespace() + ":response:"
                    + msbConf.getServiceDetails().getInstanceId());
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
