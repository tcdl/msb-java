package io.github.tcdl.msb.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.github.tcdl.msb.support.Utils.TOPIC_ANNOUNCE;
import static io.github.tcdl.msb.support.Utils.TOPIC_HEARTBEAT;
import static io.github.tcdl.msb.support.Utils.isServiceTopic;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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

    @Test
    public void testJsonDeserializationFromNull() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleBean bean = Utils.fromJson(null, SimpleBean.class, objectMapper);
        assertNull(bean);
    }

    @Test
    public void testJsonDeserializationFromEmptyString() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleBean bean = Utils.fromJson("", SimpleBean.class, objectMapper);
        assertNull(bean);
    }

    @Test
    public void testConvert() {
        int VALUE = 10;

        ObjectMapper objectMapper = new ObjectMapper();
        TestPojo pojo = new TestPojo();
        pojo.field = VALUE;

        Map map = Utils.convert(pojo, Map.class, objectMapper);

        assertEquals(VALUE, map.get("field"));
    }

    @Test(expected = JsonConversionException.class)
    public void testConversionError() {
        ObjectMapper objectMapper = new ObjectMapper();
        TestPojo pojo = new TestPojo();
        pojo.field = 10;

        Utils.convert(pojo, Integer.class, objectMapper);
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.awaitTermination(anyInt(), eq(TimeUnit.SECONDS)))
            .thenReturn(false, false, true);
        Utils.gracefulShutdown(executorService, "any");
        verify(executorService, times(1)).shutdown();
        verify(executorService, times(3)).awaitTermination(anyInt(), eq(TimeUnit.SECONDS));

    }

    @Test
    public void testGracefulShutdownInterrupted() throws Exception {
        ExecutorService executorService = mock(ExecutorService.class);
        doThrow(InterruptedException.class)
                .when(executorService)
                .awaitTermination(anyInt(), eq(TimeUnit.SECONDS));
        Utils.gracefulShutdown(executorService, "any");
        verify(executorService, times(1)).shutdown();
        verify(executorService, times(1)).awaitTermination(anyInt(), eq(TimeUnit.SECONDS));
    }

}