package io.github.tcdl.msb.acceptance.bdd.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import io.github.tcdl.msb.acceptance.MsbTestHelper;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.Utils;
import org.junit.Assert;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.github.tcdl.msb.acceptance.MsbTestHelper.DEFAULT_CONTEXT_NAME;

public class CucumberSteps {

    MsbTestHelper helper = MsbTestHelper.getInstance();

    private Config config = ConfigFactory.load();

    @Given("start MSB")
    public void initMSB() {
        helper.initWithConfig(config);
    }

    @Then("shutdown MSB")
    public void shutdownMSB() {
        helper.shutdown();
    }

    private Requester<RestPayload> requester;
    private Map<String, Object> receivedResponse;
    private CompletableFuture<Map<String, Object>> receivedResponseFuture;
    private int countRequestsReceived = 0;
    private Optional<String> nextRequestAckType = Optional.empty();
    private Optional<String> defaultRequestsAckType = Optional.empty();
    private boolean isResponseInNewThread = false;


    // responder steps
    @Given("responder server on namespace \"([^\"]*)\"")
    public void createResponderServer(String namespace) {
        createResponderServer(DEFAULT_CONTEXT_NAME, namespace);
    }

    private void createResponderServer(String contextName, String namespace) {
        ObjectMapper mapper = helper.getPayloadMapper(contextName);
        countRequestsReceived = 0;
        nextRequestAckType = Optional.empty();
        defaultRequestsAckType = Optional.empty();
        isResponseInNewThread = false;

        String responseBody = "{\"response\" : \"test-response\"}";
        helper.createResponderServer(contextName, namespace, (request, responderContext) -> {
            if (responseBody != null) {
                countRequestsReceived++;

                Runnable responseActions = () -> {
                    boolean isSendResponse = true;
                    String ackType = nextRequestAckType.orElseGet(
                            () -> defaultRequestsAckType.orElseGet(
                                    () -> "auto"));

                    switch (ackType) {
                        case "confirm":
                            responderContext.getAcknowledgementHandler().confirmMessage();
                            break;
                        case "reject":
                            responderContext.getAcknowledgementHandler().rejectMessage();
                            isSendResponse = false;
                            break;
                        case "retry":
                            responderContext.getAcknowledgementHandler().retryMessage();
                            isSendResponse = false;
                            break;
                    }

                    nextRequestAckType = Optional.empty();

                    if (isSendResponse) {
                        RestPayload payload = new RestPayload.Builder<Object, Object, Object, Map>()
                                .withBody(Utils.fromJson(responseBody, Map.class, mapper))
                                .build();
                        responderContext.getResponder().send(payload);
                    }
                };

                if(isResponseInNewThread) {
                    responderContext.getAcknowledgementHandler().setAutoAcknowledgement(false);
                    new Thread(responseActions).run();
                } else {
                    responseActions.run();
                }
            }
        }).listen();
    }

    // requester steps
    @Given("requester for namespace \"([^\"]*)\"")
    public void createRequester(String namespace) {
        createRequester(DEFAULT_CONTEXT_NAME, namespace);
    }

    private void createRequester(String contextName, String namespace) {
        requester = helper.createRequester(contextName, namespace, 1, RestPayload.class);
    }

    @When("requester sends a request")
    public void sendRequest() throws Exception {
        sendRequest(DEFAULT_CONTEXT_NAME);
    }

    private void sendRequest(String contextName) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload("QUERY", null);
        helper.sendRequest(requester, payload, 1, this::onResponse);
    }

    private void onBeforeRequest() {
        receivedResponse = null;
        receivedResponseFuture = new CompletableFuture<>();
    }

    private void onResponse(RestPayload<Object, Object, Object, Map<String, Object>> payload) {
        receivedResponseFuture.complete(payload.getBody());
    }

    @Then("requester gets response in (\\d+) ms")
    public void waitForResponse(long timeout) throws Exception {
        try {
            receivedResponse = receivedResponseFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeoutException) {
            Assert.fail("Response has not been received during a timeout");
        }
        Assert.assertNotNull("Response received is null", receivedResponse);
    }
}
