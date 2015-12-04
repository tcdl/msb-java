package io.github.tcdl.msb;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerTest {

    private static final String TOPIC = "test:consumer";

    @Mock
    private ConsumerAdapter adapterMock;

    @Mock
    private MsbConfig msbConfMock;

    @Mock
    private ChannelMonitorAgent channelMonitorAgentMock;

    @Mock
    private MessageHandler messageHandlerMock;

    private Clock clock = Clock.systemDefaultZone();

    private JsonValidator validator = new JsonValidator();

    private ObjectMapper messageMapper = TestUtils.createMessageMapper();

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullAdapter() {
        new Consumer(null, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullTopic() {
        new Consumer(adapterMock, null, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMessageHandler() {
        new Consumer(adapterMock, TOPIC, null, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMsbConf() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, null, clock, channelMonitorAgentMock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullClock() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, null, channelMonitorAgentMock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMonitorAgent() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, null, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullValidator() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, null, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMessageMapper() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, null);
    }

    @Test
    public void testSubscribeAdapterSubscribed() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);

        verify(adapterMock).subscribe(any(ConsumerAdapter.RawMessageHandler.class));
    }

    @Test
    public void testValidMessageProcessedBySubscriber() throws JsonConversionException {
        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), null);

        verify(messageHandlerMock).handleMessage(any(Message.class), any());
    }

    @Test
    public void testExceptionWhileMessageConvertingProcessedBySubscriber() throws JsonConversionException {
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);

        consumer.handleRawMessage("{\"body\":\"fake message\"}", createAcknowledgementHandler());

        verify(messageHandlerMock, never()).handleMessage(any(Message.class), any());
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicSkipValidation() {
        MsbConfig msbConf = spy(TestUtils.createMsbConfigurations());

        // disable validation
        when(msbConf.isValidateMessage()).thenReturn(false);

        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);

        // create a message with required empty namespace
        Message message = TestUtils.createSimpleRequestMessage("");

        consumer.handleRawMessage(Utils.toJson(message, messageMapper), null);

        // should skip validation and process it
        verify(messageHandlerMock).handleMessage(any(Message.class), any());
    }


    @Test
    public void testHandleRawMessageConsumeFromTopic() throws JsonConversionException {
        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), null);
        verify(messageHandlerMock).handleMessage(any(Message.class), any());
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicValidateThrowException() {
        MsbConfig msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConf, clock, channelMonitorAgentMock, validator, messageMapper);

        consumer.handleRawMessage("{\"body\":\"fake message\"}", createAcknowledgementHandler());
        verify(messageHandlerMock, never()).handleMessage(any(Message.class), any()); // no processing
    }

    @Test
    public void testHandleRawMessageConsumeFromServiceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfig msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, service_topic, messageHandlerMock, msbConf, clock, channelMonitorAgentMock, validator, messageMapper);

        consumer.handleRawMessage("{\"body\":\"fake message\"}", createAcknowledgementHandler());
        verify(messageHandlerMock, never()).handleMessage(any(Message.class), any()); // no processing
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicExpiredMessage() throws JsonConversionException {
        Message expiredMessage = createExpiredMsbRequestMessageWithTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(expiredMessage, messageMapper), createAcknowledgementHandler());
        verify(messageHandlerMock, never()).handleMessage(any(Message.class), any());
    }

    private  Message createExpiredMsbRequestMessageWithTopicTo(String topicTo) {
        Instant MOMENT_IN_PAST = Instant.parse("2007-12-03T10:15:30.00Z");

        MsbConfig msbConf = new MsbConfig(ConfigFactory.load());
        Clock clock = Clock.fixed(MOMENT_IN_PAST, ZoneId.systemDefault());

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessage.Builder metaBuilder = new MetaMessage.Builder(0, clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.Builder().withCorrelationId(Utils.generateId()).withId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder)
                .build();
    }
    
    private ConsumerAdapter.AcknowledgementHandler createAcknowledgementHandler() {
        return new ConsumerAdapter.AcknowledgementHandler() {
            @Override
            public void confirmMessage() {
            }

            @Override
            public void rejectMessage() {
            }
        };
    }
}