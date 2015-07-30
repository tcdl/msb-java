package io.github.tcdl.msb.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import org.junit.Test;

import java.time.Instant;

import static io.github.tcdl.msb.support.Utils.TOPIC_ANNOUNCE;
import static io.github.tcdl.msb.support.Utils.TOPIC_HEARTBEAT;
import static io.github.tcdl.msb.support.Utils.isServiceTopic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleBean bean = new SimpleBean();
        bean.setField("value");
        String json = Utils.toJson(bean, objectMapper);

        assertEquals("{\"field\":\"value\"}", json);
    }

    @Test
    public void testJsonDeserializationWithDefaultMapper() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleBean bean = Utils.fromJson("{\"field\":\"value\"}", SimpleBean.class, objectMapper);

        assertEquals("value", bean.getField());
    }
}