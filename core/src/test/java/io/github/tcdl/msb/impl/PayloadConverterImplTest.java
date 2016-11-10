package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.PayloadConverter;
import io.github.tcdl.msb.message.payload.Body;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class PayloadConverterImplTest {

    private PayloadConverter payloadConverter;

    @Before
    public void setUp() {
        MsbContext msbContext = new MsbContextBuilder()
                .withPayloadMapper(new ObjectMapper())
                .enableShutdownHook(true)
                .build();
        payloadConverter = msbContext.getObjectFactory().getPayloadConverter();
    }

    @Test
    public void testGetAsWithClass() {
        Map bodyAsMap = ImmutableMap.of("body", "value");
        Assert.assertEquals(bodyAsMap.get("body"), payloadConverter.getAs(bodyAsMap, Body.class).getBody());
    }

    @Test
    public void testGetAsWithTypeReference() {
        String bodyValue = "transferred content";
        Map<String, Map<String, String>> rawObject = ImmutableMap.of("key1", ImmutableMap.of("body", bodyValue));

        Map<String, Body> deserializedObjectMap = payloadConverter.getAs(rawObject, new TypeReference<Map<String, Body>>() {});
        Assert.assertEquals(1, deserializedObjectMap.size());
        Assert.assertTrue(deserializedObjectMap.containsKey("key1"));
        Assert.assertEquals(bodyValue, deserializedObjectMap.get("key1").getBody());
    }
}
