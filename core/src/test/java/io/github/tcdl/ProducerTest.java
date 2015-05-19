package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.xml.ws.Holder;

import org.junit.Test;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;

/**
 * Created by rdro on 4/28/2015.
 */
public class ProducerTest {

    @Test
    public void test_publish() {
        final Holder<Boolean> messagePublishedEventFired = new Holder<>();
        final Holder<Message> messageHolder = new Holder<Message>();
        final Holder<Exception> exceptionHolder = new Holder<>();

        Producer producer = new Producer("produceTopic", MsbConfigurations.msbConfiguration())
                .withMessageHandler((Message message, Exception exception) -> {
                        messagePublishedEventFired.value = true;
                        messageHolder.value = message;
                        exceptionHolder.value = exception;
                });
        Message message = TestUtils.createMsbResponseMessage();
        producer.publish(message);

        assertTrue(messagePublishedEventFired.value);
        assertEquals(message, messageHolder.value);
        assertNull(exceptionHolder.value);
    }
}
