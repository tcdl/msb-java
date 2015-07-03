package io.github.tcdl;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.github.tcdl.RequesterImpl;
import io.github.tcdl.adapters.mock.MockAdapter;
import io.github.tcdl.api.RequestOptions;
import io.github.tcdl.api.exception.JsonSchemaValidationException;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.api.message.payload.Payload;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author ruk
 * 
 * Component test for requester message generation validation
 */
public class RequesterIT {

    private final static String NAMESPACE = "test:requester";
    private static final Logger LOG = LoggerFactory.getLogger(RequesterIT.class);

    private RequestOptions requestOptions;
    private MsbContextImpl msbContext;
    private JsonValidator validator;

    @Before
    public void setUp() throws Exception {
        this.requestOptions = TestUtils.createSimpleRequestOptions();
        this.msbContext = TestUtils.createSimpleMsbContext();
        this.validator = new JsonValidator();
    }

    @Test
    public void testRequestMessage() throws Exception {
        String namespace = "test:requester";
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        RequesterImpl requester = RequesterImpl.create(namespace, requestOptions, msbContext);
        requester.publish(requestPayload);
        Message message = requester.getMessage();

        assertEquals("Message payload not match sent", requestPayload, message.getPayload());

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        assertRequestMessage(adapterJsonMessage, message);
    }

    private void assertRequestMessage(String json, Message message) {

        try {
            validator.validate(json, this.msbContext.getMsbConfig().getSchema());
            JSONObject jsonObject = new JSONObject(json);

            // payload fields set 
            assertTrue("Message not contain 'body' field", jsonObject.getJSONObject("payload").has("body")); 
            assertTrue("Message not contain 'headers' field", jsonObject.getJSONObject("payload").has("headers"));            
            
            // payload fields match sended 
            assertEquals("Message 'body' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(message.getPayload().getBodyAs(Map.class)),
                    jsonObject.getJSONObject("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(message.getPayload().getHeaders()), jsonObject
                    .getJSONObject("payload").get("headers").toString());

            //topics
            assertJsonContains(jsonObject.getJSONObject("topics"), "to", NAMESPACE);
            assertJsonContains(jsonObject.getJSONObject("topics"), "response", NAMESPACE + ":response:"
                    + this.msbContext.getMsbConfig().getServiceDetails().getInstanceId());

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
