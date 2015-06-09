package io.github.tcdl;

/**
 * Created by rdro on 4/28/2015.
 */

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.Adapter.RawMessageHandler;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;
import java.util.function.Function;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;

/**
 * Created by rdro on 4/28/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsumerTest {

    private static final String TOPIC = "test:consumer";

    @Mock
    private Adapter adapterMock;

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
    public void testConsumeFromTopicValidateThrowException() {
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConf, clock, channelMonitorAgentMock)
                .subscribe(subscriberMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(subscriberMock).handleMessage(any(), isA(JsonSchemaValidationException.class));
    }

    @Test
    public void testConsumeFromSeviceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfigurations msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, service_topic, msbConf, clock, channelMonitorAgentMock)
                .subscribe(subscriberMock);

        consumer.handleRawMessage("{\"body\":\"fake message\"}");
        verify(subscriberMock).handleMessage(any(), isA(JsonConversionException.class));
    }

    @Test
    public void testConsumeFromTopic() throws JsonConversionException {
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, TOPIC, msbConfMock, clock, channelMonitorAgentMock)
                .subscribe(subscriberMock);

        consumer.handleRawMessage(Utils.toJson(originalMessage));
        verify(subscriberMock).handleMessage(any(Message.class), any());
    }
}