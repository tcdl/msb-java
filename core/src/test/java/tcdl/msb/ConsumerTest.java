package tcdl.msb;

/**
 * Created by rdro on 4/28/2015.
 */

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.xml.ws.Holder;

import org.junit.Before;
import org.junit.Test;

import tcdl.msb.adapters.MockAdapter;
import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.events.TwoArgumentsAdapter;
import tcdl.msb.messages.Message;
import tcdl.msb.support.TestUtils;
import tcdl.msb.support.Utils;

/**
 * Created by rdro on 4/28/2015.
 */
public class ConsumerTest {

	private MsbMessageOptions config;

    @Before
    public void setUp() {
        config = TestUtils.createSimpleConfig();
    }

	@Test
	public void testConsume() {
		final Holder<Boolean> messageConsumedEventFired = new Holder<Boolean>();
		final Holder<Message> messageHolder = new Holder<Message>();
		final Holder<Exception> exceptionHolder = new Holder<Exception>();

		new Consumer(config.getNamespace(), MsbConfigurations.msbConfiguration(), new MsbMessageOptions())
			.withMessageHandler(new TwoArgumentsAdapter<Message, Exception>() {
					@Override
					public void onEvent(Message message, Exception exception) {
						messageConsumedEventFired.value = true;
						messageHolder.value = message;
						exceptionHolder.value = exception;
					}
			}).subscribe();
        
        MockAdapter.getInstance().consume(Utils.toJson(TestUtils.createSimpleMsbMessage()));

		assertTrue(messageConsumedEventFired.value);
		assertNotNull(messageHolder.value);
		assertNull(exceptionHolder.value);
	}
}
