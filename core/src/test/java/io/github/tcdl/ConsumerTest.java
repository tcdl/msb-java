package io.github.tcdl;

/**
 * Created by rdro on 4/28/2015.
 */
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.Adapter.RawMessageHandler;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
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
    private Adapter adapterMock;

    @Mock
    private TwoArgsEventHandler<Message, Exception> handlerMock;

    @Mock
    private MsbConfigurations msbConfMock;

    @Captor
    ArgumentCaptor<RawMessageHandler> messageHandlerCaptur;

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullAdapter() {
        new Consumer(null, TOPIC, handlerMock, msbConfMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullTopic() {
        new Consumer(adapterMock, null, handlerMock, msbConfMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullHandler() {
        new Consumer(adapterMock, TOPIC, null, msbConfMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMsbConf() {
        new Consumer(adapterMock, TOPIC, handlerMock, null);
    }

    @Test
    public void testConsumeFromTopicValidateThrowException() {
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();
        new Consumer(adapterMock, TOPIC, handlerMock, msbConf).subscribe();

        verify(adapterMock).subscribe(messageHandlerCaptur.capture());
        messageHandlerCaptur.getValue().onMessage("{\"body\":\"fake message\"}");
        verify(handlerMock).onEvent(eq(null), isA(JsonSchemaValidationException.class));
    }

    @Test
    public void testConsumeFromSeviceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();

        new Consumer(adapterMock, service_topic, handlerMock, msbConf).subscribe();

        verify(adapterMock).subscribe(messageHandlerCaptur.capture());
        messageHandlerCaptur.getValue().onMessage("{\"body\":\"fake message\"}");
        verify(handlerMock).onEvent(eq(null), isA(JsonConversionException.class));
    }

    @Test
    public void testConsumeFromTopic() throws JsonConversionException {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);

        new Consumer(adapterMock, TOPIC, handlerMock, msbConfMock).subscribe();

        verify(adapterMock).subscribe(messageHandlerCaptur.capture());
        messageHandlerCaptur.getValue().onMessage(Utils.toJson(originaMessage));
        verify(handlerMock).onEvent(any(Message.class), eq(null));
    }

}