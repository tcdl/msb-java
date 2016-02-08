package io.github.tcdl.msb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Created by rdro on 4/28/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class ProducerTest {

    private static final String TOPIC = "test:producer";

    @Mock
    private ProducerAdapter adapterMock;

    @Mock
    private Callback<Message> handlerMock;

    private ObjectMapper messageMapper = TestUtils.createMessageMapper();

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerProducerNullAdapter() {
        new Producer(null, "testTopic", handlerMock, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProducerNullTopic() {
        new Producer(adapterMock, null, handlerMock, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProducerNullHandler() {
        new Producer(adapterMock, "testTopic", null, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProducerNullMapper() {
        new Producer(adapterMock, "testTopic", handlerMock, null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishVerifyHandlerCalled() {
        Message originaMessage = TestUtils.createSimpleRequestMessage(TOPIC);

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock, messageMapper);
        producer.publish(originaMessage);

        verify(handlerMock).call(any(Message.class));
    }

    @Test(expected = ChannelException.class)
    @SuppressWarnings("unchecked")
    public void testPublishRawAdapterThrowChannelException() throws ChannelException {
        Message originaMessage = TestUtils.createSimpleRequestMessage(TOPIC);

        Mockito.doThrow(ChannelException.class).when(adapterMock).publish(anyString());

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock, messageMapper);
        producer.publish(originaMessage);

        verify(handlerMock, never()).call(any(Message.class));
    }

    @Test(expected = JsonConversionException.class)
    @Ignore("Need to create message that when parse to JSON will cause JsonProcessingException in Utils.toJson or use PowerMock")
    @SuppressWarnings("unchecked")
    public void testPublishThrowExceptionVerifyCallbackNotSetNotCalled() throws ChannelException {
        Message brokenMessage = createBrokenRequestMessageWithAndTopicTo(TOPIC);

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock, messageMapper);
        producer.publish(brokenMessage);

        verify(adapterMock, never()).publish(anyString());
        verify(handlerMock, never()).call(any(Message.class));
    }

    private  Message createBrokenRequestMessageWithAndTopicTo(String topicTo) {
        MsbConfig msbConf = new MsbConfig(ConfigFactory.load());
        Clock clock = Clock.systemDefaultZone();
        ObjectMapper payloadMapper = TestUtils.createMessageMapper();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId(), null);
        Map<String, String> body = new HashMap<>();
        body.put("body", "{\\\"x\\\" : 3} garbage");
        RestPayload<?, ?, ?, Map<String, String>> payload = new RestPayload.Builder<Object, Object, Object, Map<String, String>>()
                .withBody(body)
                .build();
        JsonNode payloadNode = Utils.convert(payload, JsonNode.class, payloadMapper);
        MetaMessage.Builder metaBuilder = new MetaMessage.Builder(null,  clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.Builder()
                .withCorrelationId(Utils.generateId())
                .withId(Utils.generateId())
                .withTopics(topic)
                .withMetaBuilder(metaBuilder)
                .withPayload(payloadNode)
                .build();
    }
}