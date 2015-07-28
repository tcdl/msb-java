package io.github.tcdl.msb.acceptance;

import java.util.Map;

public class SimpleResponder {

    private String namespace;
    private MsbTestHelper helper = MsbTestHelper.getInstance();

    SimpleResponder(String namespace) {
        this.namespace = namespace;
    }

    public void runSimpleResponderExample() {
        helper.initDefault();
        helper.createResponderServer(namespace, (request, responder) -> {
            System.out.print(">>> REQUEST: " + request.getBodyAs(Map.class));
            helper.sleep(500);
            helper.respond(responder, namespace + ":" + "SimpleResponder");
        })
        .listen();
    }

    public void shutdown() {
        helper.shutdown();
    }
}
