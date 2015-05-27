package io.github.tcdl.examples;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.Responder;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by rdro on 5/18/2015.
 */
public class ResponderExample {

    public static void main(String... args) {
        Config config = ConfigFactory.load();
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        ChannelManager channelManager = new ChannelManager(msbConfig);
        MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails());

        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace("test:simple-requester");

        ResponderServer.create(options, channelManager, messageFactory, msbConfig)
                .use(((request, responder) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());

                    responder.sendAck(10000, 3, null);
                    Thread.sleep(5000);

                    responder.send(createResponse(1), null);
                    responder.send(createResponse(2), null);
                    responder.send(createResponse(3), null);
                }))
                .listen();
    }

    private static Payload createResponse(int seq) {
        Map<String, String> body = new HashMap<>();
        body.put("result", "response " + seq + " from responder");
        return  new Payload.PayloadBuilder().setBody(body).build();
    }
}
