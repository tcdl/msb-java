package io.github.tcdl;

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
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    private JsonValidator validator = new JsonValidator();

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullAdapter() {
        new Consumer(null, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullTopic() {
        new Consumer(adapterMock, null, msbConfMock, clock, channelMonitorAgentMock, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMsbConf() {
        new Consumer(adapterMock, TOPIC, null, clock, channelMonitorAgentMock, validator);
    }

    @Test
    public void testValidMessageProcessedByAllSubscribers() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);

        Consumer.Subscriber subscriberMock1 = mock(Consumer.Subscriber.class);
        Consumer.Subscriber subscriberMock2 = mock(Consumer.Subscriber.class);

        consumer.subscribe(subscriberMock1);
        consumer.subscribe(subscriberMock2);

        consumer.handleRawMessage(Utils.toJson(originalMessage));

        verify(subscriberMock1).handleMessage(any(Message.class));
        verify(subscriberMock2).handleMessage(any(Message.class));
        verify(subscriberMock1, never()).handleError(any(Exception.class));
        verify(subscriberMock1, never()).handleError(any(Exception.class));
    }

    @Test
    public void testExceptionWhileMessageConvertingProcessedByAllSubscribers() throws JsonConversionException {
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        Consumer.Subscriber subscriberMock1 = mock(Consumer.Subscriber.class);
        Consumer.Subscriber subscriberMock2 = mock(Consumer.Subscriber.class);

        consumer.subscribe(subscriberMock1);
        consumer.subscribe(subscriberMock2);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");

        verify(subscriberMock1).handleError(isA(JsonConversionException.class));
        verify(subscriberMock2).handleError(isA(JsonConversionException.class));

        verify(subscriberMock1, never()).handleMessage(any(Message.class));
        verify(subscriberMock2, never()).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicSkipValidation() {
        MsbConfigurations msbConf = spy(TestUtils.createMsbConfigurations());

        // disable validation
        when(msbConf.isValidateMessage()).thenReturn(false);

        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConf, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(subscriberMock);

        // create a message with required empty namespace
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("");

        consumer.handleRawMessage(Utils.toJson(message));

        // should skip validation and process it
        verify(subscriberMock).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicValidateThrowException() {
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConf, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(subscriberMock).handleError(isA(JsonSchemaValidationException.class));
        verify(subscriberMock, never()).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromServiceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, service_topic, msbConf, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(subscriberMock).handleError(isA(JsonConversionException.class));
        verify(subscriberMock, never()).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopic() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage(Utils.toJson(originalMessage));
        verify(subscriberMock).handleMessage(any(Message.class));
        verify(subscriberMock, never()).handleError(isA(JsonConversionException.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicExpiredMessage() throws JsonConversionException {
        Message expiredMessage = createExpiredMsbRequestMessageWithTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(subscriberMock);

        consumer.handleRawMessage(Utils.toJson(expiredMessage));
        verify(subscriberMock, never()).handleMessage(any(Message.class));
        verify(subscriberMock, never()).handleError(any(Exception.class));
    }

    @Test
    public void testSubscribeUnsubscribeOne() {
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        Consumer.Subscriber subscriber = new NoopSubscriber();
        consumer.subscribe(subscriber);

        assertTrue(consumer.unsubscribe(subscriber));
    }

    @Test
    public void testSubscribeUnsubscribeMultiple() {
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        Consumer.Subscriber subscriber1 = new NoopSubscriber();
        Consumer.Subscriber subscriber2 = new NoopSubscriber();

        consumer.subscribe(subscriber1);
        consumer.subscribe(subscriber2);

        assertFalse(consumer.unsubscribe(subscriber1));
        assertTrue(consumer.unsubscribe(subscriber2));
    }

    private  Message createExpiredMsbRequestMessageWithTopicTo(String topicTo) {
        MsbConfigurations msbConf = new MsbConfigurations(ConfigFactory.load());
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessage.MetaMessageBuilder metaBuilder = new MetaMessage.MetaMessageBuilder(0, clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder)
                .build();
    }

    private static class NoopSubscriber implements Consumer.Subscriber {
        @Override
        public void handleMessage(Message message) {}

        @Override
        public void handleError(Exception exception) {}
    }
}