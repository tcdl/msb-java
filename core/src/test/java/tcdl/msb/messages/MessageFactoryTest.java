package tcdl.msb.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.messages.payload.ResponsePayload;
import tcdl.msb.support.TestUtils;
import tcdl.msb.support.Utils;

/**
 * Created by rdro on 4/23/2015.
 */
public class MessageFactoryTest {

    private MsbMessageOptions messageOptions;
    private MsbConfigurations msbConf;
    private MessageFactory messageFactory;

    @Before
    public void setUp() throws Exception {
        this.messageOptions = TestUtils.createSimpleConfig();
        this.msbConf = MsbConfigurations.msbConfiguration();
        this.messageFactory = MessageFactory.getInstance();
    }

    @Test
    public void test_createBaseMessage() throws Exception {
        Message message = messageFactory.createBaseMessage(null, false);
        String json = Utils.toJson(message);
        assertBaseMessage(json);
    }

    @Test
    public void test_createBaseMessage_from_original() throws Exception {
        Message originalMessage = TestUtils.createSimpleMsbMessage();
        Message message = messageFactory.createBaseMessage(originalMessage, false);
        String json = Utils.toJson(message);

        assertBaseMessage(json);
        JSONObject jsonObject = new JSONObject(json);
        assertJsonContains(jsonObject, "correlationId", originalMessage.getCorrelationId());
    }

    @Test
    public void testCreateRequestMessage() throws Exception {
        Message message = messageFactory.createRequestMessage(messageOptions, null);
        String json = Utils.toJson(message);

        assertBaseMessage(json);
        JSONObject jsonObject = new JSONObject(json);
        assertJsonContains(jsonObject.getJSONObject("topics"), "to", messageOptions.getNamespace());
        assertJsonContains(jsonObject.getJSONObject("topics"), "response", messageOptions.getNamespace() + ":response:"
                + msbConf.getServiceDetails().getInstanceId());
    }

    @Test
    public void testCreateResponseMessage() throws Exception {
        // todo
        Message originalMessage = TestUtils.createSimpleMsbMessage().withAck(null).withPayload(null);

        Acknowledge ack = new Acknowledge().withResponderId(Utils.generateId());

        ResponsePayload payload = TestUtils.createSimpleResponsePayload();

        Message message = messageFactory.createResponseMessage(originalMessage, ack, payload);

        assertEquals("Message 'To' field not set correctly", originalMessage.getTopics().getResponse(), message
                .getTopics().getTo());
        assertEquals("Message 'Ack' field not set correctly", ack, message.getAck());
        assertEquals("Message 'Payload' field not set correctly", payload, message.getPayload());
    }

    @Test
    public void testCreateAckMessage() throws Exception {
        Message originalMessage = TestUtils.createSimpleMsbMessage().withAck(null).withPayload(null);

        Acknowledge ack = new Acknowledge().withResponderId(Utils.generateId());

        Message message = messageFactory.createAckMessage(originalMessage, ack);

        assertEquals("Message 'To' field not set correctly", originalMessage.getTopics().getResponse(), message
                .getTopics().getTo());
        assertEquals("Message 'Ack' field not set correctly", ack, message.getAck());
        assertNull("Message 'Payload' field should be empty", message.getPayload());
    }

    @Test
    public void test_createAck() throws Exception {
        Acknowledge ack = messageFactory.createAck(messageOptions);
        assertNotNull(ack.getResponderId());
    }

    @Test
    public void test_createMeta() throws Exception {
        MetaMessage meta = messageFactory.createMeta(messageOptions);

        assertEquals(messageOptions.getTtl(), meta.getTtl());
        assertEquals("Message 'create_at' is not equals now", DateUtils.truncate(new Date(), Calendar.SECOND),
                DateUtils.truncate(meta.getCreatedAt(), Calendar.SECOND));

        assertNotNull(meta.getServiceDetails());
        assertEquals(meta.getServiceDetails().getName(), msbConf.getServiceDetails().getName());
        assertEquals(meta.getServiceDetails().getVersion(), msbConf.getServiceDetails().getVersion());
        assertEquals(meta.getServiceDetails().getInstanceId(), msbConf.getServiceDetails().getInstanceId());
        assertEquals(meta.getServiceDetails().getHostname(), msbConf.getServiceDetails().getHostName());
        assertEquals(meta.getServiceDetails().getIp(), msbConf.getServiceDetails().getIp());
        assertEquals((int) meta.getServiceDetails().getPid(), msbConf.getServiceDetails().getPid());
    }

    @Test
    @Ignore("createdAt validation failed")
    public void test_completeMeta() throws Exception {
        int delay = 10000;
        MetaMessage meta = messageFactory.createMeta(messageOptions).withCreatedAt(
                new Date(System.currentTimeMillis() - delay));
        Message message = messageFactory.createBaseMessage(null, true).withMeta(meta);
        message = messageFactory.completeMeta(message, meta);
        String json = Utils.toJson(message);

        assertBaseMessage(json);
        assertTrue(message.getMeta().getDurationMs().intValue() - delay < 10);
    }

    private void assertBaseMessage(String json) throws Exception {
        assertTrue(Utils.validateJsonWithSchema(json, this.msbConf.getSchema()));
        JSONObject jsonObject = new JSONObject(json);
        assertTrue(jsonObject.has("id"));
        assertTrue(jsonObject.has("correlationId"));
    }

    private void assertJsonContains(JSONObject jsonObject, String field, Object value) {
        assertTrue(jsonObject.has(field));
        assertNotNull(jsonObject.get(field));
        assertEquals(value, jsonObject.get(field));
    }
}
