package io.github.tcdl;

import io.github.tcdl.adapters.MockAdapter;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Created by rdro on 4/30/2015.
 */
public class ResponderServerTest {

    // TODO write tests
    @Test
    public void testResponderServer() throws Exception {
        Message message = TestUtils.createMsbRequestMessageNoPayload();
        MockAdapter.getInstance().publish(Utils.toJson(message));
        
        MsbMessageOptions msgOptions = TestUtils.createSimpleConfig();
        Responder.createServer(msgOptions).use((request, response) -> {
            System.out.println("Processor 1");
            System.out.println("body:" + response.getBody());
            Map<String, String> body = new HashMap<>();
            body.put("key", "value");
            response.setBody(body);
        }).use((request, response) -> {
            System.out.println("Processor 2");
            System.out.println("body:" + response.getBody());
        }).use((request, response) -> {
            response.getResponder().sendAck(500, null, null);
        }).listen();
    }
}
