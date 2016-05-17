package io.github.tcdl.msb.acceptance;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleResponder {

    private String namespace;
    private MsbTestHelper helper = MsbTestHelper.getInstance();

    SimpleResponder(String namespace) {
        this.namespace = namespace;
    }

    public void runSimpleResponderExample() {
        helper.initDefault();
        helper.createResponderServer(namespace, (request, responderContext) -> {
            System.out.println(">>> REQUEST: " + request);
            responderContext.getResponder().send(namespace + ":" + "SimpleResponder");
        }, String.class)
        .listen();
    }

    public void shutdown() {
        helper.shutdown();
    }
}
