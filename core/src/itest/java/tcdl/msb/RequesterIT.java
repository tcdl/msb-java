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
/** * 
 * @author ruk
 * 
 * Component test for requester message generation validation
 */
public class RequesterIT {

    private MsbMessageOptions messageOptions;
    private MsbConfigurations msbConf;  

    @Before
    public void setUp() throws Exception {
        this.messageOptions = TestUtils.createSimpleConfig();
        this.msbConf = MsbConfigurations.msbConfiguration();        
    }

    @Test
    public void testCreateRequestMessage() throws Exception {
        Requester requester = new Requester(messageOptions, null);
        requester.publish(TestUtils.createSimpleRequestPayload());
        Message message = requester.getMessage();
        String json = Utils.toJson(message);

        assertRequestMessage(json);       
    }

    private void assertRequestMessage(String json) throws Exception {
        assertTrue("Message didn't correspondent to expected schema",
                Utils.validateJsonWithSchema(json, this.msbConf.getSchema()));
        JSONObject jsonObject = new JSONObject(json);
        
        // payload        
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("payload").has("params"));
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("payload").has("headers"));
        assertTrue("Message not contain expected property", jsonObject.getJSONObject("payload").has("body"));
        
        //topics
        assertJsonContains(jsonObject.getJSONObject("topics"), "to", messageOptions.getNamespace());
        assertJsonContains(jsonObject.getJSONObject("topics"), "response", messageOptions.getNamespace() + ":response:"
                + msbConf.getServiceDetails().getInstanceId());
    }

    private void assertJsonContains(JSONObject jsonObject, String field, Object value) {
        assertTrue(jsonObject.has(field));
        assertNotNull(jsonObject.get(field));
        assertEquals(value, jsonObject.get(field));
    }

}
