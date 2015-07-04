package io.github.tcdl.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.tcdl.MsbContextImpl;
import io.github.tcdl.adapters.mock.MockAdapter;
import io.github.tcdl.api.exception.JsonSchemaValidationException;
import io.github.tcdl.api.message.payload.Payload;
import io.github.tcdl.impl.RequesterImpl;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.json.JSONException;
import org.json.JSONObject;
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

    @Before
    public void setUp() throws Exception {
        this.requestOptions = TestUtils.createSimpleRequestOptions();
        this.msbContext = TestUtils.createSimpleMsbContext();
        this.validator = new JsonValidator();
    }

    @Test
    public void testRequestMessage() throws Exception {
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        RequesterImpl requester = RequesterImpl.create(NAMESPACE, requestOptions, msbContext);
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
            assertEquals("Message 'body' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(payload.getBodyAs(Map.class)),
                    jsonObject.getJSONObject("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", Utils.getMsbJsonObjectMapper().writeValueAsString(payload.getHeaders()), jsonObject
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
