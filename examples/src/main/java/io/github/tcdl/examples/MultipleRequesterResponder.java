package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

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
    private MsbContext msbContext;

    private MsbMessageOptions optionsResponder = new MsbMessageOptions();
    private MsbMessageOptions optionsRequester1 = new MsbMessageOptions();
    private MsbMessageOptions optionsRequester2 = new MsbMessageOptions();

    MultipleRequesterResponder(MsbContext msbContext, String responderNamespace, String requesterNamespace1,
                               String requesterNamespace2) {
        this.msbContext = msbContext;
        optionsResponder.setNamespace(responderNamespace);
        optionsRequester1.setNamespace(requesterNamespace1);
        optionsRequester2.setNamespace(requesterNamespace2);
    }

    public MsbContext getMsbContext() {
        return msbContext;
    }

    public void setMsbContext(MsbContext msbContext) {
        this.msbContext = msbContext;
    }

    public void runMultipleRequesterResponder() {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("MultipleRequesterResponder-%d")
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(2, threadFactory);

        ResponderServer.create(optionsResponder, msbContext)
                .use(((request, responder) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());

                    Future<String> futureRequester1 = createAndRunRequester(optionsRequester1, msbContext, executor, "user1@example.com");
                    Future<String> futureRequester2 = createAndRunRequester(optionsRequester2, msbContext, executor, "user2@example.com");

                    Thread.sleep(500);

                    String result1 = futureRequester1.get();
                    String result2 = futureRequester2.get();

                    executor.shutdownNow();

                    responder.send(createResponse(result1 + result2));
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
