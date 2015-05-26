package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

/**
 * Created by anstr on 5/26/2015.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Responder.class)
public class ResponderTest {
    private static MsbMessageOptions config;
    private static Responder responder;
    private static Message message;
    private static String topic;

    @BeforeClass
    public static void setUp() {
        config = TestUtils.createSimpleConfig();
        topic = config.getNamespace();
        message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        responder = new Responder(config, message);
    }

    @Test(expected = NullPointerException.class)
    public void testResponderNullConfigsNullMessageThrowsException() {
        new Responder(null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testResponderNullConfigsThrowsException() {
        new Responder(null, message);
    }

    @Test(expected = NullPointerException.class)
    public void testResponderNullMessageThrowsException() {
        new Responder(config, null);
    }

    @Test
    public void testResponderNotNullOk() {
        new Responder(config, message);
    }

    @Test
    public void testResponderSendAckOk() throws Exception {
        Responder spy = PowerMockito.spy(responder);

        spy.sendAck(200, 2, null);

        PowerMockito.verifyPrivate(spy, times(1)).invoke("sendMessage", anyObject(), anyObject());
    }

    @Test
    public void testResponderSendOk() throws Exception {
        Responder spy = PowerMockito.spy(responder);

        Payload payload = new Payload.PayloadBuilder().build();
        spy.send(payload, null);

        PowerMockito.verifyPrivate(spy, times(1)).invoke("sendMessage", anyObject(), anyObject());
    }
}
