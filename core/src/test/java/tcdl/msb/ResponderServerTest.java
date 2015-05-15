package tcdl.msb;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import tcdl.msb.adapters.MockAdapter;
import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.messages.Message;
import tcdl.msb.messages.Topics;
import tcdl.msb.support.TestUtils;
import tcdl.msb.support.Utils;

/**
 * Created by rdro on 4/30/2015.
 */
public class ResponderServerTest {

	// TODO write tests
	@Test
	public void testResponderServer() throws Exception {
		MsbMessageOptions msgOptions = TestUtils.createSimpleConfig();
		Responder.createServer(msgOptions).use((request, response) -> {
			System.out.println("Processor 1");
			System.out.println("body:" + response.getBody());
			Map body = new HashMap();
			body.put("key", "value");
			response.withBody(body);
		}).use((request, response) -> {
			System.out.println("Processor 2");
			System.out.println("body:" + response.getBody());
		}).use((request, response) -> {
			response.getResponder().sendAck(500, null, null);
		}).listen();

		Message message = TestUtils.createSimpleMsbMessage().withTopics(
				new Topics().withTo(msgOptions.getNamespace()).withResponse(msgOptions.getNamespace()));
		MockAdapter.getInstance().consume(Utils.toJson(message));
	}
}
