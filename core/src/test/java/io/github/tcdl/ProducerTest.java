package io.github.tcdl;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.messages.Topics;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

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

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerProducerNullAdapter() {
        new Producer(null, "testTopic", handlerMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProducerNullTopic() {
        new Producer(adapterMock, null, handlerMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullHandler() {
        new Producer(adapterMock, "testTopic", null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishVerifyHandlerCalled() {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage);

        verify(handlerMock).call(any(Message.class));
    }

    @Test(expected = ChannelException.class)
    @SuppressWarnings("unchecked")
    public void testPublishRawAdapterThrowChannelException() throws ChannelException {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);

        Mockito.doThrow(ChannelException.class).when(adapterMock).publish(anyString());

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage);

        verify(handlerMock, never()).call(any(Message.class));
    }

    @Test(expected = JsonConversionException.class)
    @Ignore("Need to create message that when parse to JSON will cause JsonProcessingException in Utils.toJson or use PowerMock")
    @SuppressWarnings("unchecked")
    public void testPublishThrowExceptionVerifyCallbackNotSetNotCalled() throws ChannelException {
        Message brokenMessage = createBrokenRequestMessageWithAndTopicTo(TOPIC);

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(brokenMessage);

        verify(adapterMock, never()).publish(anyString());
        verify(handlerMock, never()).call(any(Message.class));
    }

    private  Message createBrokenRequestMessageWithAndTopicTo(String topicTo) {
        MsbConfigurations msbConf = new MsbConfigurations(ConfigFactory.load());
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        Map<String, String> body = new HashMap<String, String>();
        body.put("body", "{\\\"x\\\" : 3} garbage");
        Payload payload = new Payload.PayloadBuilder().setBody(body).build();
        MetaMessage.MetaMessageBuilder metaBuilder = new MetaMessage.MetaMessageBuilder(null,  clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(
                metaBuilder).withPayload(payload)
                .build();
    }
}