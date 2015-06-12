package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleResponderExample {
    private MsbContext msbContext;
    private MsbMessageOptions options = new MsbMessageOptions();

    SimpleResponderExample(MsbContext msbContext, String namespace) {
        this.msbContext = msbContext;
        options.setNamespace(namespace);
    }

    public void runSimpleResponderExample() {
        ResponderServer.create(options, msbContext)
                .use(((request, responder) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());

                    Thread.sleep(500);

                    responder.send(createResponse(options.getNamespace()));
                }))
                .listen();
    }

    private static Payload createResponse(String message) {
        Map<String, String> body = new HashMap<>();
        body.put("SimpleResponderExample", message);
        return new Payload.PayloadBuilder().setBody(body).build();
    }

    public void setMsbContext(MsbContext msbContext) {
        this.msbContext = msbContext;
    }

    public MsbContext getMsbContext() {
        return msbContext;
    }
}
