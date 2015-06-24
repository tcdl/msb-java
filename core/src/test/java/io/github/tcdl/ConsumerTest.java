package io.github.tcdl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Clock;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ConsumerAdapter.RawMessageHandler;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.JsonConversionException;
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
    private MessageHandler messageHandlerMock;

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
    public void testValidMessageProcessedBySubscriber() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);

        MessageHandler messageHandlerMock = mock(MessageHandler.class);

        consumer.subscribe(messageHandlerMock);

        consumer.handleRawMessage(Utils.toJson(originalMessage));

        verify(messageHandlerMock).handleMessage(any(Message.class));
    }

    @Test
    public void testExceptionWhileMessageConvertingProcessedByAllSubscribers() throws JsonConversionException {
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        MessageHandler messageHandlerMock1 = mock(MessageHandler.class);
        MessageHandler messageHandlerMock2 = mock(MessageHandler.class);

        consumer.subscribe(messageHandlerMock1);
        consumer.subscribe(messageHandlerMock2);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");

        verify(messageHandlerMock1, never()).handleMessage(any(Message.class));
        verify(messageHandlerMock2, never()).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicSkipValidation() {
        MsbConfigurations msbConf = spy(TestUtils.createMsbConfigurations());

        // disable validation
        when(msbConf.isValidateMessage()).thenReturn(false);

        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConf, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(messageHandlerMock);

        // create a message with required empty namespace
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("");

        consumer.handleRawMessage(Utils.toJson(message));

        // should skip validation and process it
        verify(messageHandlerMock).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicValidateThrowException() {
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConf, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(messageHandlerMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(messageHandlerMock, never()).handleMessage(any(Message.class)); // no processing
    }

    @Test
    public void testHandleRawMessageConsumeFromServiceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, service_topic, msbConf, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(messageHandlerMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(messageHandlerMock, never()).handleMessage(any(Message.class)); // no processing
    }

    @Test
    public void testHandleRawMessageConsumeFromTopic() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(messageHandlerMock);

        consumer.handleRawMessage(Utils.toJson(originalMessage));
        verify(messageHandlerMock).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicExpiredMessage() throws JsonConversionException {
        Message expiredMessage = createExpiredMsbRequestMessageWithTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock, validator);
        consumer.subscribe(messageHandlerMock);

        consumer.handleRawMessage(Utils.toJson(expiredMessage));
        verify(messageHandlerMock, never()).handleMessage(any(Message.class));
    }

    private  Message createExpiredMsbRequestMessageWithTopicTo(String topicTo) {
        MsbConfigurations msbConf = new MsbConfigurations(ConfigFactory.load());
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessage.MetaMessageBuilder metaBuilder = new MetaMessage.MetaMessageBuilder(0, clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder)
                .build();
    }
}