package io.github.tcdl.msb.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.mock.MockAdapter;
import io.github.tcdl.msb.api.exception.JsonSchemaValidationException;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component test for requester message generation validation
 */
public class RequesterIT {

    private final static String NAMESPACE = "test:requester";
    private static final Logger LOG = LoggerFactory.getLogger(RequesterIT.class);

    private RequestOptions requestOptions;
    private MsbContextImpl msbContext;
    private JsonValidator validator;
    private ObjectMapper messageMapper;

    @Before
    public void setUp() throws Exception {
        this.requestOptions = TestUtils.createSimpleRequestOptions();
        this.msbContext = TestUtils.createSimpleMsbContext();
        this.validator = new JsonValidator();
        this.messageMapper = msbContext.getPayloadMapper();
    }

    @Test
    public void testRequestMessage() throws Exception {
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(NAMESPACE, requestOptions, null);
        requester.publish(requestPayload);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        assertRequestMessage(adapterJsonMessage, requestPayload);
    }

    private void assertRequestMessage(String json, Payload payload) {

        try {
            validator.validate(json, this.msbContext.getMsbConfig().getSchema());
            JSONObject jsonObject = new JSONObject(json);

            // payload fields set 
            assertTrue("Message not contain 'body' field", jsonObject.getJSONObject("payload").has("body")); 
            assertTrue("Message not contain 'headers' field", jsonObject.getJSONObject("payload").has("headers"));            
            
            // payload fields match sent
            Assert.assertEquals("Message 'body' is incorrect", messageMapper.writeValueAsString(payload.getBody()),
                    jsonObject.getJSONObject("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", messageMapper.writeValueAsString(payload.getHeaders()), jsonObject
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
