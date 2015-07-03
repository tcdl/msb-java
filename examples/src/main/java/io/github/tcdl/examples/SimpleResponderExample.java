package io.github.tcdl.examples;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleResponderExample {

    private String namespace;
    private MSBUtil util = MSBUtil.getInstance();

    SimpleResponderExample(String namespace) {
        this.namespace = namespace;
    }

    public void runSimpleResponderExample() {
        util.createResponderServer(namespace, (request, responder) -> {
            System.out.print(">>> REQUEST: " + request.getHeaders());
            util.sleep(500);
            util.respond(responder, namespace + ":" + "SimpleResponderExample");
        })
        .listen();
    }

    public void shutdown() {
        util.shutdown();
    }
}
