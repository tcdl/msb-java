package io.github.tcdl.examples;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleResponderExample extends BaseExample {

    private String namespace;

    SimpleResponderExample(String namespace) {
        this.namespace = namespace;
    }

    public void runSimpleResponderExample() {
        createResponderServer(namespace,(request, responder) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());
                    sleep(500);
                    respond(responder, namespace + ":" + "SimpleResponderExample");
                })
                .listen();
    }
}
