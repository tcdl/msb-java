package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by anstr on 6/9/2015.
 */
public class MultipleRequesterResponder {
    public static void main(String... args) {
        MsbMessageOptions optionsResponder = new MsbMessageOptions();
        MsbMessageOptions optionsRequester1 = new MsbMessageOptions();
        MsbMessageOptions optionsRequester2 = new MsbMessageOptions();

        if (args.length != 3) {
            System.out.println("Please run MultipleRequesterResponder in the following format:");
            System.out.println("Example:java RequesterExample test:simple-queue1 test:simple-queue2 test:simple-queue3");
            System.exit(1);
        } else {
            optionsResponder.setNamespace(args[0]);
            optionsRequester1.setNamespace(args[1]);
            optionsRequester2.setNamespace(args[2]);
        }

        MsbContext msbContext = new MsbContext.MsbContextBuilder().build();

        ExecutorService executor = Executors.newFixedThreadPool(5);

        ResponderServer.create(optionsResponder, msbContext)
                .use(((request, responder) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());

                    Future<String> futureRequester1 = createAndRunRequester(optionsRequester1, msbContext, executor, "user1@example.com");
                    Future<String> futureRequester2 = createAndRunRequester(optionsRequester2, msbContext, executor, "user2@example.com");

                    Thread.sleep(500);

                    String result1=futureRequester1.get();
                    String result2=futureRequester2.get();

                    responder.send(createResponse(result1+result2));
                }))
                .listen();
    }

    private static Payload createResponse(String message) {
        Map<String, String> body = new HashMap<>();
        body.put("result", "response from MultipleRequesterResponder:" + message);
        return new Payload.PayloadBuilder().setBody(body).build();
    }

    private static Future<String> createAndRunRequester(MsbMessageOptions options, MsbContext msbContext,
                                                        ExecutorService executor, String email) {
        options.setWaitForResponses(1);
        options.setResponseTimeout(5000);

        Map<String, String> headers = new HashMap<>();
        headers.put("From", email);
        Payload requestPayload = new Payload.PayloadBuilder().setHeaders(headers).build();
        Requester requester = Requester.create(options, msbContext);

        Future<String> future = executor.submit(new Callable<String>() {
            String result = null;

            @Override
            public String call() throws Exception {
                requester
                        .onResponse(payload -> {
                                    System.out.println(">>> RESPONSE body: " + payload.getBody());
                                    result = payload.getBody().toString();
                                    synchronized (this) {
                                        notify();
                                    }
                                }

                        );
                requester.publish(requestPayload);

                synchronized (this) {
                    wait();
                }

                return result;
            }
        });
        return future;
    }
}
