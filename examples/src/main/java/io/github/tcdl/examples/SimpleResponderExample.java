package io.github.tcdl.examples;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleResponderExample {

    private String namespace;
    private MsbTestHelper helper = MsbTestHelper.getInstance();

    SimpleResponderExample(String namespace) {
        this.namespace = namespace;
    }

    public void runSimpleResponderExample() {
        helper.initDefault();
        helper.createResponderServer(namespace, (request, responder) -> {
            System.out.print(">>> REQUEST: " + request.getHeaders());
            helper.sleep(500);
            helper.respond(responder, namespace + ":" + "SimpleResponderExample");
        })
        .listen();
    }

    public void shutdown() {
        helper.shutdown();
    }
}
