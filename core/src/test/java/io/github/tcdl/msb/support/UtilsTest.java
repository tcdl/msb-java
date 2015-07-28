package io.github.tcdl.msb.support;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.message.payload.MyPayload;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;

import static io.github.tcdl.msb.support.Utils.TOPIC_ANNOUNCE;
import static io.github.tcdl.msb.support.Utils.TOPIC_HEARTBEAT;
import static io.github.tcdl.msb.support.Utils.isServiceTopic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class UtilsTest {
    @Test
    public void testIsServiceTopic() {
        assertTrue(isServiceTopic(TOPIC_ANNOUNCE));
        assertTrue(isServiceTopic(TOPIC_HEARTBEAT));
        assertTrue(isServiceTopic("_channels:heartbeat:response:3c4aef9c53df1f4900013fe4"));

        assertFalse(isServiceTopic("search:parsers:facets:v1"));
        assertFalse(isServiceTopic("search:parsers:facets:v1:response:3c4ae26a27521c0e0001498a"));
    }

    @Test
    public void testInstantJsonSerialization() throws JsonConversionException {
        String instantStr = "2015-06-02T15:50:08.039Z";
        Instant instant = Instant.parse(instantStr);

        String serializedInstant = Utils.toJson(instant, TestUtils.createMessageMapper());
        assertEquals("\"" + instantStr + "\"", serializedInstant);
    }

    @Test
    public void testJsonSerializationWithDefaultMapper() throws Exception {
        ObjectMapper defaultMapper = ((MsbContextImpl) new MsbContextBuilder().build()).getMessageMapper();

        SimpleBean bean = new SimpleBean();
        bean.setField("value");
        String json = Utils.toJson(bean, defaultMapper);

        assertEquals("{\"field\":\"value\"}", json);
    }

    @Test
    public void testJsonDeserializationWithDefaultMapper() throws Exception {
        ObjectMapper defaultMapper = ((MsbContextImpl) new MsbContextBuilder().build()).getMessageMapper();

        SimpleBean bean = Utils.fromJson("{\"field\":\"value\"}", SimpleBean.class, defaultMapper);

        assertEquals("value", bean.getField());
    }

    @Test
    public void testJsonSerializationWithCustomPayloadMapper() throws Exception {
        ObjectMapper payloadMapperMock = mock(ObjectMapper.class);
        ObjectMapper messageMapper = ((MsbContextImpl) new MsbContextBuilder()
                .withPayloadMapper(payloadMapperMock)
                .build()).getMessageMapper();

        Message message = TestUtils.createMsbRequestMessageWithSimplePayload(TestUtils.getSimpleNamespace());
        Payload payload = message.getPayload();

        Utils.toJson(message, messageMapper);
        verify(payloadMapperMock).writeValueAsString(payload);
    }

    @Test
    public void testJsonDeserializationWithCustomPayloadMapper() throws Exception {
        ObjectMapper payloadMapperSpy = spy(new ObjectMapper());
        ObjectMapper messageMapper = ((MsbContextImpl) new MsbContextBuilder()
                .withPayloadMapper(payloadMapperSpy)
                .build()).getMessageMapper();

        Message message = TestUtils.createMsbRequestMessageWithSimplePayload(TestUtils.getSimpleNamespace());
        String jsonMessage = Utils.toJson(message, messageMapper);
        String jsonPayload = Utils.toJson(message.getPayload(), payloadMapperSpy);

        Utils.fromJson(jsonMessage, Message.class, messageMapper);
        verify(payloadMapperSpy).readValue(jsonPayload, Payload.class);
    }

    @Test
    public void testToCustomParametricType() throws Exception {
        ObjectMapper messageMapper = ((MsbContextImpl) new MsbContextBuilder().build()).getMessageMapper();

        Message message = TestUtils.createMsbRequestMessageWithSimplePayload(TestUtils.getSimpleNamespace());
        Message<MyPayload> customizedMessage = Utils.toCustomParametricType(message, Message.class, MyPayload.class, messageMapper);

        assertTrue(customizedMessage.getPayload() instanceof MyPayload);
    }

    @Test
    public void testToCustomTypeReference() {
        int transferedValue = 10;
        Map<String, Map<String, Integer>> rawObject = ImmutableMap.of("key1", ImmutableMap.of("field", transferedValue));

        Map<String,TestPojo> deserializedObjectMap = Utils.toCustomTypeReference(rawObject, new TypeReference<Map<String, TestPojo>>() {
        }, TestUtils.createMessageMapper());
        assertEquals(1, deserializedObjectMap.size());
        assertTrue(deserializedObjectMap.containsKey("key1"));
        assertEquals(transferedValue, deserializedObjectMap.get("key1").field);
    }
}