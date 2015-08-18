package io.github.tcdl.msb.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Base64;
import io.github.tcdl.msb.adapters.mock.MockAdapter;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Component test for requester message generation validation
 */
public class RequesterIT {

    private final static String NAMESPACE = "test:requester";
    private final static String STATIC_TAG = "requester-tag";

    private RequestOptions requestOptions;
    private MsbContextImpl msbContext;

    @Before
    public void setUp() throws Exception {
        this.requestOptions = TestUtils.createSimpleRequestOptionsWithTags(STATIC_TAG);
        this.msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testRequestMessage() throws Exception {
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(NAMESPACE, requestOptions);
        requester.publish(requestPayload);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        TestUtils.assertRequestMessagePayload(adapterJsonMessage, requestPayload, NAMESPACE);
    }

    @Test
    public void testRequestMessageWithBodyBufferBase64Encoded() throws Exception {
        byte[] bytesToSend = new byte[] { 1, 2 };
        Payload requestPayload = TestUtils.createSimpleRequestPayloadWithBodyBuffer(bytesToSend);
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(NAMESPACE, requestOptions);
        requester.publish(requestPayload);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        JsonNode jsonObject = msbContext.getPayloadMapper().readTree(adapterJsonMessage);
        
        String base64Encoded = Base64.getEncoder().encodeToString(bytesToSend);
        TestUtils.assertJsonContains(jsonObject.get("payload"), "bodyBuffer", base64Encoded);
    }

    @Test
    public void testRequestMessageWithDynamicTag() throws Exception {
        String dynamicTag = "dynamic-tag";
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(NAMESPACE, requestOptions);
        requester.publish(requestPayload, dynamicTag);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        TestUtils.assertRequestMessagePayload(adapterJsonMessage, requestPayload, NAMESPACE);
        TestUtils.assertMessageTags(adapterJsonMessage, STATIC_TAG, dynamicTag);
    }

    @Test
    public void testRequestMessageWithDynamicTagAndOriginalMessage() throws Exception {
        String dynamicTag = "dynamic-tag";
        String dynamicTagOriginal = "dynamic-tag-original";
        Message originalMessage = TestUtils.createSimpleRequestMessageWithTags(NAMESPACE, dynamicTagOriginal);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(NAMESPACE, requestOptions);
        requester.publish(requestPayload, originalMessage, dynamicTag);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        TestUtils.assertRequestMessagePayload(adapterJsonMessage, requestPayload, NAMESPACE);
        TestUtils.assertMessageTags(adapterJsonMessage, dynamicTagOriginal, STATIC_TAG, dynamicTag);
    }

    @Test
    public void testRequestMessageWithDuplicateTagInOriginalMessage() throws Exception {
        String dynamicTag = "dynamic-tag";
        String dynamicTagOriginal = "dynamic-tag-original";
        Message originalMessage = TestUtils.createSimpleRequestMessageWithTags(NAMESPACE, dynamicTagOriginal, STATIC_TAG);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(NAMESPACE, requestOptions);
        requester.publish(requestPayload, originalMessage, dynamicTag);

        String adapterJsonMessage = MockAdapter.pollJsonMessageForTopic(NAMESPACE);
        TestUtils.assertRequestMessagePayload(adapterJsonMessage, requestPayload, NAMESPACE);
        TestUtils.assertMessageTags(adapterJsonMessage, dynamicTagOriginal, STATIC_TAG, dynamicTag);
    }
}
