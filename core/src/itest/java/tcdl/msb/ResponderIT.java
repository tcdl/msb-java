package tcdl.msb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.messages.Message;
import tcdl.msb.support.TestUtils;
import tcdl.msb.support.Utils;

public class ResponderIT {

    private MsbMessageOptions messageOptions;
    private MsbConfigurations msbConf;

    @Before
    public void setUp() throws Exception {
        this.messageOptions = TestUtils.createSimpleConfig();
        this.msbConf = MsbConfigurations.msbConfiguration();
    }

    @Test
    public void testCreateAckMessage() throws Exception {
        Responder responder = new Responder(messageOptions, TestUtils.createSimpleMsbMessage());
        responder.sendAck(1000, 2, null);
        Message message = responder.getResponseMessage();
        String json = Utils.toJson(message);

        assertAckMessage(json);
    }

    private void assertAckMessage(String json) throws Exception {
        assertTrue("Message didn't correspondent to expected schema",
                Utils.validateJsonWithSchema(json, this.msbConf.getSchema()));
        JSONObject jsonObject = new JSONObject(json);

        // ack  
        assertTrue("Message not contain expected property", jsonObject.has("ack"));
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("ack").has("responderId"));
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("ack").has("responsesRemaining"));
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("ack").has("timeoutMs"));

        //topics
        assertJsonContains(jsonObject.getJSONObject("topics"), "to", messageOptions.getNamespace() + ":response:"
                + msbConf.getServiceDetails().getInstanceId());
        assertTrue(jsonObject.getJSONObject("topics").isNull("response"));
    }

    @Test
    public void testCreateResponseMessage() throws Exception {
        Responder responder = new Responder(messageOptions, TestUtils.createSimpleMsbMessage());
        responder.send(TestUtils.createSimpleResponsePayload(), null);
        Message message = responder.getResponseMessage();
        String json = Utils.toJson(message);

        assertResponseMessage(json);
    }

    private void assertResponseMessage(String json) throws Exception {
        assertTrue("Message didn't correspondent to expected schema",
                Utils.validateJsonWithSchema(json, this.msbConf.getSchema()));
        JSONObject jsonObject = new JSONObject(json);

        // payload        
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("payload").has("statusCode"));
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("payload").has("headers"));
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("payload").has("body"));

        //topics
        assertJsonContains(jsonObject.getJSONObject("topics"), "to", messageOptions.getNamespace() + ":response:"
                + msbConf.getServiceDetails().getInstanceId());
        assertTrue(jsonObject.getJSONObject("topics").isNull("response"));
    }

    private void assertJsonContains(JSONObject jsonObject, String field, Object value) {
        assertTrue(jsonObject.has(field));
        assertNotNull(jsonObject.get(field));
        assertEquals(value, jsonObject.get(field));
    }

}
