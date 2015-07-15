package io.github.tcdl.msb;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.ChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.Utils;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
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
    private MsbConfig msbConfMock;

    @Mock
    private ChannelMonitorAgent channelMonitorAgentMock;

    @Mock
    private MessageHandler messageHandlerMock;

    private Clock clock = Clock.systemDefaultZone();

    private JsonValidator validator = new JsonValidator();

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullAdapter() {
        new Consumer(null, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullTopic() {
        new Consumer(adapterMock, null, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMessageHandler() {
        new Consumer(adapterMock, TOPIC, null, msbConfMock, clock, channelMonitorAgentMock, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMsbConf() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, null, clock, channelMonitorAgentMock, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullClock() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, null, channelMonitorAgentMock, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMonitorAgent() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, null, validator);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullValidator() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, null);
    }

    @Test
    public void testSubscribeAdapterSubscribed() {
        new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);

        verify(adapterMock).subscribe(any(ConsumerAdapter.RawMessageHandler.class));
    }

    @Test
    public void testValidMessageProcessedBySubscriber() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);

        consumer.handleRawMessage(Utils.toJson(originalMessage));

        verify(messageHandlerMock).handleMessage(any(Message.class));
    }

    @Test
    public void testExceptionWhileMessageConvertingProcessedBySubscriber() throws JsonConversionException {
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");

        verify(messageHandlerMock, never()).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicSkipValidation() {
        MsbConfig msbConf = spy(TestUtils.createMsbConfigurations());

        // disable validation
        when(msbConf.isValidateMessage()).thenReturn(false);

        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);

        // create a message with required empty namespace
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("");

        consumer.handleRawMessage(Utils.toJson(message));

        // should skip validation and process it
        verify(messageHandlerMock).handleMessage(any(Message.class));
    }


    @Test
    public void testHandleRawMessageConsumeFromTopic() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);

        consumer.handleRawMessage(Utils.toJson(originalMessage));
        verify(messageHandlerMock).handleMessage(any(Message.class));
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicValidateThrowException() {
        MsbConfig msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConf, clock, channelMonitorAgentMock, validator);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(messageHandlerMock, never()).handleMessage(any(Message.class)); // no processing
    }

    @Test
    public void testHandleRawMessageConsumeFromServiceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfig msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, service_topic, messageHandlerMock, msbConf, clock, channelMonitorAgentMock, validator);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(messageHandlerMock, never()).handleMessage(any(Message.class)); // no processing
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicExpiredMessage() throws JsonConversionException {
        Message expiredMessage = createExpiredMsbRequestMessageWithTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, messageHandlerMock, msbConfMock, clock, channelMonitorAgentMock, validator);

        consumer.handleRawMessage(Utils.toJson(expiredMessage));
        verify(messageHandlerMock, never()).handleMessage(any(Message.class));
    }

    private  Message createExpiredMsbRequestMessageWithTopicTo(String topicTo) {
        Instant MOMENT_IN_PAST = Instant.parse("2007-12-03T10:15:30.00Z");

        MsbConfig msbConf = new MsbConfig(ConfigFactory.load());
        Clock clock = Clock.fixed(MOMENT_IN_PAST, ZoneId.systemDefault());

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessage.Builder metaBuilder = new MetaMessage.Builder(0, clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.Builder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder)
                .build();
    }
}