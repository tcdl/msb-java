package io.github.tcdl.msb.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.mock.MockAdapter;
import io.github.tcdl.msb.api.exception.JsonSchemaValidationException;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Component test for requester message generation validation
 */
public class RequesterIT {

    private final static String NAMESPACE = "test:requester";
    private static final Logger LOG = LoggerFactory.getLogger(RequesterIT.class);

    private RequestOptions requestOptions;
    private MsbContextImpl msbContext;
    private JsonValidator validator;
    private ObjectMapper payloadMapper;

    @Before
    public void setUp() throws Exception {
        this.requestOptions = TestUtils.createSimpleRequestOptions();
        this.msbContext = TestUtils.createSimpleMsbContext();
        this.validator = new JsonValidator();
        this.payloadMapper = msbContext.getPayloadMapper();
    }

    @Test
    public void testRequestMessage() throws Exception {
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(NAMESPACE, requestOptions);
        requester.publish(requestPayload);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        assertRequestMessage(adapterJsonMessage, requestPayload);
    }

    private void assertRequestMessage(String json, Payload payload) {
        try {
            validator.validate(json, this.msbContext.getMsbConfig().getSchema());
            JsonNode jsonObject = payloadMapper.readTree(json);

            // payload fields set 
            assertTrue("Message not contain 'body' field", jsonObject.get("payload").has("body"));
            assertTrue("Message not contain 'headers' field", jsonObject.get("payload").has("headers"));
            
            // payload fields match sent
            assertEquals("Message 'body' is incorrect", payloadMapper.writeValueAsString(payload.getBody()),
                    jsonObject.get("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", payloadMapper.writeValueAsString(payload.getHeaders()), jsonObject
                    .get("payload").get("headers").toString());

            // topics
            TestUtils.assertJsonContains(jsonObject.get("topics"), "to", NAMESPACE);
            TestUtils.assertJsonContains(jsonObject.get("topics"), "response", NAMESPACE + ":response:"
                    + this.msbContext.getMsbConfig().getServiceDetails().getInstanceId());

        } catch (JsonSchemaValidationException | IOException e) {
            LOG.error("Exception while parse message payload", e);
            fail("Message validation failed");
        }
    }
}
