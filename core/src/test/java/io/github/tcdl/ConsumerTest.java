package io.github.tcdl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.time.Clock;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ConsumerAdapter.RawMessageHandler;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.messages.Topics;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by rdro on 4/28/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsumerTest {

    private static final String TOPIC = "test:consumer";

    @Mock
    private ConsumerAdapter adapterMock;

    @Mock
    private MsbConfigurations msbConfMock;

    @Mock
    private ChannelMonitorAgent channelMonitorAgentMock;

    @Captor
    private ArgumentCaptor<RawMessageHandler> messageHandlerCaptor;

    @Mock
    private Consumer.Subscriber subscriberMock;

    private Clock clock = Clock.systemDefaultZone();

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullAdapter() {
        new Consumer(null, TOPIC, msbConfMock, clock, channelMonitorAgentMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullTopic() {
        new Consumer(adapterMock, null, msbConfMock, clock, channelMonitorAgentMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMsbConf() {
        new Consumer(adapterMock, TOPIC, null, clock, channelMonitorAgentMock);
    }

    @Test
    public void testValidMessageProcessedByAllSubscribers() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock);

        Consumer.Subscriber subscriberMock1 = mock( Consumer.Subscriber.class);
        Consumer.Subscriber subscriberMock2 = mock( Consumer.Subscriber.class);

        consumer.subscribe(subscriberMock1);
        consumer.subscribe(subscriberMock2);

        consumer.handleRawMessage(Utils.toJson(originalMessage));

        verify(subscriberMock1).handleMessage(any(Message.class), eq(null));
        verify(subscriberMock2).handleMessage(any(Message.class), eq(null));
    }

    @Test
    public void testExceptionWhileMessageConvertingProcessedByAllSubscribers() throws JsonConversionException {
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock);
        Consumer.Subscriber subscriberMock1 = mock( Consumer.Subscriber.class);
        Consumer.Subscriber subscriberMock2 = mock( Consumer.Subscriber.class);

        consumer.subscribe(subscriberMock1);
        consumer.subscribe(subscriberMock2);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");

        verify(subscriberMock1).handleMessage(any(Message.class), isA(JsonConversionException.class));
        verify(subscriberMock2).handleMessage(any(Message.class), isA(JsonConversionException.class));
    }


    @Test
    public void testHandleRawMessageConsumeFromTopicValidateThrowException() {
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConf, clock, channelMonitorAgentMock);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(subscriberMock).handleMessage(any(Message.class), isA(JsonSchemaValidationException.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromServiceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, service_topic, msbConf, clock, channelMonitorAgentMock);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(subscriberMock).handleMessage(any(Message.class), isA(JsonConversionException.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopic() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage(Utils.toJson(originalMessage));
        verify(subscriberMock).handleMessage(any(Message.class), eq(null));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicExpiredMessage() throws JsonConversionException {
        Message expiredMessage = createExpiredMsbRequestMessageWithAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage(Utils.toJson(expiredMessage));
        verify(subscriberMock, never()).handleMessage(any(Message.class), eq(null));
    }

    @Test
    public void testSubscribeUnsubscribeOne() {
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock);
        Consumer.Subscriber subscriber = (message, exception) -> {};
        consumer.subscribe(subscriber);

        assertTrue(consumer.unsubscribe(subscriber));
    }

    @Test
    public void testSubscribeUnsubscribeMultiple() {
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock);
        Consumer.Subscriber subscriber1 = (message, exception) -> {};
        Consumer.Subscriber subscriber2 = (message, exception) -> {};

        consumer.subscribe(subscriber1);
        consumer.subscribe(subscriber2);

        assertFalse(consumer.unsubscribe(subscriber1));
        assertTrue(consumer.unsubscribe(subscriber2));
    }

    private  Message createExpiredMsbRequestMessageWithAndTopicTo(String topicTo) {
        MsbConfigurations msbConf = new MsbConfigurations(ConfigFactory.load());
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics.TopicsBuilder().setTo(topicTo)
                .setResponse(topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage.MetaMessageBuilder metaBuilder = new MetaMessage.MetaMessageBuilder(0, clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder)
                .build();
    }
}