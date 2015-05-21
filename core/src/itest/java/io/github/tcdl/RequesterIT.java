package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author ruk
 * 
 *         Component test for requester message generation validation
 */
public class RequesterIT {

    public static final Logger LOG = LoggerFactory.getLogger(RequesterIT.class);

    private MsbMessageOptions messageOptions;
    private MsbConfigurations msbConf;

    @Before
    public void setUp() throws Exception {
        this.messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester");
        this.msbConf = MsbConfigurations.msbConfiguration();
    }

    @Test
    public void testRequestMessage() throws Exception {
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester requester = new Requester(messageOptions, null);
        requester.publish(requestPayload);
        Message message = requester.getMessage();

        assertEquals("Message payload not match sended", requestPayload, message.getPayload());

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(messageOptions.getNamespace());
        assertRequestMessage(adapterJsonMessage, message);
    }

    private void assertRequestMessage(String json, Message message) {

        try {
            Utils.validateJsonWithSchema(json, this.msbConf.getSchema());
            JSONObject jsonObject = new JSONObject(json);

            // payload fields set 
            assertTrue("Message not contain 'body' field", jsonObject.getJSONObject("payload").has("body")); 
            assertTrue("Message not contain 'headers' field", jsonObject.getJSONObject("payload").has("headers"));            
            
            // payload fields match sended 
            assertEquals("Message 'body' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(message.getPayload().getBody()),
                    jsonObject.getJSONObject("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(message.getPayload().getHeaders()), jsonObject
                    .getJSONObject("payload").get("headers").toString());

            //topics
            assertJsonContains(jsonObject.getJSONObject("topics"), "to", messageOptions.getNamespace());
            assertJsonContains(jsonObject.getJSONObject("topics"), "response", messageOptions.getNamespace() + ":response:"
                    + msbConf.getServiceDetails().getInstanceId());

        } catch (JsonSchemaValidationException | JsonProcessingException | JSONException e) {
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
