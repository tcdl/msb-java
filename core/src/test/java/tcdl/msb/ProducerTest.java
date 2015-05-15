package tcdl.msb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.xml.ws.Holder;

import org.junit.Test;

import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.events.TwoArgumentsAdapter;
import tcdl.msb.messages.Message;
import tcdl.msb.support.TestUtils;

/**
 * Created by rdro on 4/28/2015.
 */
public class ProducerTest {	

	@Test
	public void test_publish() {
		final Holder<Boolean> messagePublishedEventFired = new Holder<>();
		final Holder<Message> messageHolder = new Holder<Message>();
		final Holder<Exception> exceptionHolder = new Holder<>();

		Producer producer = new Producer("consumeTopic", MsbConfigurations.msbConfiguration())
				.withMessageHandler(new TwoArgumentsAdapter<Message, Exception>() {
						@Override
						public void onEvent(Message message, Exception exception) {
							messagePublishedEventFired.value = true;
							messageHolder.value = message;
							exceptionHolder.value = exception;
						}
				});
		Message message = TestUtils.createSimpleMsbMessage();
		producer.publish(message);

		assertTrue(messagePublishedEventFired.value);
		assertEquals(message, messageHolder.value);
		assertNull(exceptionHolder.value);
	}
}
