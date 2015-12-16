package io.github.tcdl.msb.acceptance.bdd.steps;

import static io.github.tcdl.msb.acceptance.MsbTestHelper.DEFAULT_CONTEXT_NAME;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.Utils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.Matchers;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;
import org.jbehave.core.model.ExamplesTable;
import org.jbehave.core.model.OutcomesTable;
import org.junit.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Steps to send requests and respond with predifined responses
 */
public class RequesterResponderSteps extends MsbSteps {

    private volatile Requester<RestPayload> requester;
    private volatile String responseBody;
    private volatile Map<String, Object> receivedResponse;
    private volatile CompletableFuture<Map<String, Object>> receivedResponseFuture;
    private volatile CountDownLatch responseCountDown;
    private volatile AtomicInteger countRequestsReceived;
    private volatile AtomicInteger countResponsesReceived;
    private volatile Optional<String> nextRequestAckType = Optional.empty();
    private volatile Optional<String> defaultRequestsAckType = Optional.empty();
    private volatile boolean isResponseInNewThread = false;
    private volatile int responseProcessingDelay;
    private volatile int responsesToSendCount;
    private volatile int responsesToExpectCount;

    public Optional<String> getDefaultRequestsAckType() {
        return defaultRequestsAckType;
    }

    // responder steps
    @Given("responder server listens on namespace $namespace")
    public void createResponderServer(String namespace) {
        createResponderServer(DEFAULT_CONTEXT_NAME, namespace);
    }

    @Given("responder server from $contextName listens on namespace $namespace")
    @When("responder server from $contextName listens on namespace $namespace")
    public void createResponderServer(String contextName, String namespace) {
        beforeCreateResponder();
        ObjectMapper mapper = helper.getPayloadMapper(contextName);
        helper.createResponderServer(contextName, namespace, (request, responderContext) -> {
            if (responseBody != null) {
                countRequestsReceived.incrementAndGet();

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
                        for(int i=0; i< responsesToSendCount; i++) {
                            responderContext.getResponder().send(payload);
                        }
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

    private void beforeCreateResponder() {
        responseCountDown = null;
        countRequestsReceived = new AtomicInteger(0);
        countResponsesReceived = new AtomicInteger(0);
        responseProcessingDelay = 0;
        responsesToSendCount = 1;
        responsesToExpectCount = 1;
        nextRequestAckType = Optional.empty();
        defaultRequestsAckType = Optional.empty();
        isResponseInNewThread = false;
    }

    @Given("responder server will $nextRequestAckType next request")
    public void setNextRequestAckType(String nextRequestAckType) throws Exception {
        this.nextRequestAckType = Optional.of(nextRequestAckType);
    }

    @Given("responder server will $allRequestsAckType all requests")
    public void setDefaultRequestsAckType(String allRequestsAckType) throws Exception {
        this.defaultRequestsAckType = Optional.of(allRequestsAckType);
    }

    @Given("requester will process responses with $timeout ms delay")
    public void setesponseDelay(int responseDelay) throws Exception {
        this.responseProcessingDelay = responseDelay;
    }

    @Given("responder will provide $responseCount responses")
    public void setResponsesToSendCount(int responsesToSendCount) throws Exception {
        this.responsesToSendCount = responsesToSendCount;
    }

    @Given("responder server will send acknowledge and response from a new thread")
    public void setResponseInNewThread() throws Exception {
        isResponseInNewThread = true;
    }

    @Given("responder server responds with '$body'")
    @When("responder server responds with '$body'")
    public void respond(String body) {
        responseBody = body;
    }

    // requester steps
    @Given("requester sends requests to namespace $namespace")
    public void createRequester(String namespace) {
        createRequester(DEFAULT_CONTEXT_NAME, namespace);
    }

    // requester steps
    @Given("requester (with $requestTimeout ms request timeout to receive $responseCount responses) sends requests to namespace $namespace")
    public void createRequester(int requestTimeout, int responseCount, String namespace) {
        responseCountDown = new CountDownLatch(responseCount);
        responsesToExpectCount = responseCount;
        requester = helper.createRequester(DEFAULT_CONTEXT_NAME, namespace, responsesToExpectCount, 100, requestTimeout, RestPayload.class);
    }

    @Given("requester from $contextName sends requests to namespace $namespace")
    public void createRequester(String contextName, String namespace) {
        requester = helper.createRequester(contextName, namespace, 1, RestPayload.class);
    }

    @When("requester sends a request")
    public void sendRequest() throws Exception {
        sendRequest(DEFAULT_CONTEXT_NAME);
    }

    @When("requester from $contextName sends a request")
    public void sendRequest(String contextName) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload("QUERY", null);
        helper.sendRequest(requester, payload, responsesToExpectCount, this::onResponse);
    }

    @When("requester sends a request with query '$query'")
    public void sendRequestWithQuery(String query) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload(query, null);
        helper.sendRequest(requester, payload, true, responsesToExpectCount, null, this::onResponse);
    }

    @When("requester sends a request with body '$body'")
    public void sendRequestWithBody(String body) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload(null, body);
        helper.sendRequest(requester, payload, true, responsesToExpectCount, null, this::onResponse);
    }

    private void onBeforeRequest() {
        receivedResponse = null;
        receivedResponseFuture = new CompletableFuture<>();
    }

    private void onResponse(RestPayload<Object, Object, Object, Map<String, Object>> payload) {
        if(responseProcessingDelay > 0) {
            try {
                Thread.sleep(responseProcessingDelay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if(responseCountDown != null) {
            responseCountDown.countDown();
        }

        countResponsesReceived.incrementAndGet();
        receivedResponseFuture.complete(payload.getBody());
    }

    @Then("requester gets response in $timeout ms")
    public void waitForResponse(long timeout) throws Exception {
        try {
            receivedResponse = receivedResponseFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeoutException) {
            Assert.fail("Response has not been received during a timeout");
        }
        Assert.assertNotNull("Response received is null", receivedResponse);
    }

    @Then("requester will get all responses in $timeout ms")
    public void waitForAllResponses(long timeout) throws Exception {
        if(!responseCountDown.await(timeout, TimeUnit.MILLISECONDS)) {
            Assert.fail("All responses has not been received during a timeout, pending count: " + responseCountDown.getCount());
        }
    }

    @Then("requester does not get a response in $timeout ms")
    public void waitForNoResponse(long timeout) throws Exception {
        try {
            receivedResponse = receivedResponseFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeoutException) {
            //ok
        }
        Assert.assertNull("Unexpected response received", receivedResponse);
    }

    @Then("response equals $table")
    public void responseEquals(ExamplesTable table) throws Exception {
        Map<String, String> expected = table.getRow(0);
        OutcomesTable outcomes = new OutcomesTable();

        for (String key : expected.keySet()) {
            outcomes.addOutcome(key, receivedResponse.get(key), Matchers.equalTo(expected.get(key)));
        }

        outcomes.verify();
    }

    @Then("responder requests received count equals $expectedRequestsReceivedCount")
    public void requestCountEquals(int expectedCountRequestsReceived) throws Exception {
        Assert.assertEquals(expectedCountRequestsReceived, countRequestsReceived.get());
    }

    @Then("requester responses received count equals $expectedRequestsReceivedCount")
    public void responseCountEquals(int expectedCountResponsesReceived) throws Exception {
        Assert.assertEquals(expectedCountResponsesReceived, countResponsesReceived.get());
    }

    @Then("response contains $table")
    public void responseContains(ExamplesTable table) throws Exception {
        Map<String, String> expected = table.getRow(0);
        OutcomesTable outcomes = new OutcomesTable();

        for (String key : expected.keySet()) {
            outcomes.addOutcome(key, receivedResponse.get(key).toString(), Matchers.containsString(expected.get(key)));
        }

        outcomes.verify();
    }
}
