package io.github.tcdl;

/**
 * Created by rdro on 4/28/2015.
 */
import static junit.framework.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.MockAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import java.util.Random;

import org.junit.Test;

/**
 * Created by rdro on 4/28/2015.
 */
public class ConsumerTest {

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullAdapter() {
        TwoArgsEventHandler<Message, Exception> adapterMock = mock(TwoArgsEventHandler.class);
        MsbConfigurations msbConf = mock(MsbConfigurations.class);
        new Consumer(null, "testTopic", adapterMock, msbConf);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullTopic() {
        Adapter adapterMock = mock(Adapter.class);
        TwoArgsEventHandler<Message, Exception> handlerMock = mock(TwoArgsEventHandler.class);
        MsbConfigurations msbConf = mock(MsbConfigurations.class);
        new Consumer(adapterMock, null, handlerMock, msbConf);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullHandler() {
        Adapter adapterMock = mock(Adapter.class);
        MsbConfigurations msbConf = mock(MsbConfigurations.class);
        new Consumer(adapterMock, "testTopic", null, msbConf);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMsbConf() {
        Adapter adapterMock = mock(Adapter.class);
        TwoArgsEventHandler<Message, Exception> handlerMock = mock(TwoArgsEventHandler.class);
        new Consumer(adapterMock, "testTopic", handlerMock, null);
    }

    @Test
    public void testConsumeFromTopicSchemaValidationFailed() {
        String cosumerTopic = "consumerTopic:" + new Random().nextInt(1000);
        TwoArgsEventHandler<Message, Exception> handlerMock = mock(TwoArgsEventHandler.class);
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();

        MockAdapter mockAdapter = MockAdapter.getInstance();
        mockAdapter.clearAllMessages();
        try {
            mockAdapter.publish("{\"body\":\"fake message\"}");
        } catch (ChannelException e) {
            fail("fail to run test");
        }

        new Consumer(mockAdapter, cosumerTopic, handlerMock, msbConf).subscribe();
        verify(handlerMock).onEvent(eq(null), isA(JsonSchemaValidationException.class));
    }

    @Test
    public void testConsumeFromSeviceTopicNoSchemaValidationFailed() {
        String cosumerTopic = "_consumerTopic:" + new Random().nextInt(1000);

        TwoArgsEventHandler<Message, Exception> handlerMock = mock(TwoArgsEventHandler.class);
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();

        MockAdapter mockAdapter = MockAdapter.getInstance();
        mockAdapter.clearAllMessages();
        try {
            mockAdapter.publish("{\"body\":\"fake message\"}");
        } catch (ChannelException e) {
            fail("fail to run test");
        }

        new Consumer(mockAdapter, cosumerTopic, handlerMock, msbConf).subscribe();
        verify(handlerMock).onEvent(eq(null), isA(JsonConversionException.class));
    }

    @Test
    public void testConsumeFromTopic() {
        String cosumerTopic = "consumerTopic:" + new Random().nextInt(1000);

        MsbConfigurations msbConf = mock(MsbConfigurations.class);
        TwoArgsEventHandler<Message, Exception> handlerMock = mock(TwoArgsEventHandler.class);

        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(cosumerTopic);
        MockAdapter mockAdapter = MockAdapter.getInstance();
        mockAdapter.clearAllMessages();
        try {
            mockAdapter.publish(Utils.toJson(originaMessage));
        } catch (ChannelException | JsonConversionException e) {
            fail("fail to run test");
        }

        new Consumer(mockAdapter, cosumerTopic, handlerMock, msbConf).subscribe();

        verify(handlerMock).onEvent(any(Message.class), eq(null));       
    }

}