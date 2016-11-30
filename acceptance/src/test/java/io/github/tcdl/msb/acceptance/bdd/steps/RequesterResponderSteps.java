package io.github.tcdl.msb.acceptance.bdd.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.acceptance.MsbTestHelper;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;
import org.jbehave.core.model.ExamplesTable;
import org.jbehave.core.model.OutcomesTable;
import org.junit.Assert;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.github.tcdl.msb.acceptance.MsbTestHelper.DEFAULT_CONTEXT_NAME;
import static org.junit.Assert.*;

/**
 * Steps to send requests and respond with predefined responses
 */
public class RequesterResponderSteps {

    protected final MsbTestHelper helper = MsbTestHelper.getInstance();

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
    private volatile String latestForwardNamespace = null;
    private CompletableFuture<RestPayload> lastFutureResult = null;
    private RestPayload resolvedResponse = null;
    private final LinkedList<Map<String, Object>> responses = new LinkedList<>();
    private final String ACK = "ACK";
    private final String PAYLOAD = "PAYLOAD";
    private final int ACK_TIMEOUT = 500;
    private final Map<String, List<String>> receivedMessagesByConsumer = new ConcurrentHashMap<>();
    private final Deque<Message> rawIncomingMessages = new ConcurrentLinkedDeque<>();

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
            if (responseBody == null) {
                return;
            }

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
                latestForwardNamespace = responderContext.getOriginalMessage().getTopics().getForward();
                if (isSendResponse) {
                    RestPayload payload = new RestPayload.Builder<Object, Object, Object, Map>()
                            .withBody(Utils.fromJson(responseBody, Map.class, mapper))
                            .build();
                    for (int i = 0; i < responsesToSendCount; i++) {
                        responderContext.getResponder().send(payload);
                    }
                }
            };

            if (isResponseInNewThread) {
                responderContext.getAcknowledgementHandler().setAutoAcknowledgement(false);
                new Thread(responseActions).run();
            } else {
                responseActions.run();
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
    @Given("requester sets forwarding to $forwardNamespace and target namespace to $namespace")
    public void createRequesterWithForwarding(String forwardNamespace, String namespace) {
        requester = helper.createRequester(DEFAULT_CONTEXT_NAME, namespace, forwardNamespace, 0, RestPayload.class);
    }

    // requester steps
    @Given("requester (with $requestTimeout ms request timeout to receive $responseCount responses) sends requests to namespace $namespace")
    public void createRequester(int requestTimeout, int responseCount, String namespace) {
        responseCountDown = new CountDownLatch(responseCount);
        responsesToExpectCount = responseCount;
        requester = helper.createRequester(DEFAULT_CONTEXT_NAME, namespace, null, responsesToExpectCount, 100, requestTimeout, RestPayload.class);
    }

    @Given("requester from $contextName sends requests to namespace $namespace")
    public void createRequester(String contextName, String namespace) {
        requester = helper.createRequester(contextName, namespace, null, 1, RestPayload.class);
    }

    @When("requester sends a request")
    public void sendRequest() throws Exception {
        sendRequest(DEFAULT_CONTEXT_NAME);
    }

    @When("requester from $contextName sends a request")
    public void sendRequest(String contextName) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload("QUERY", null);
        helper.sendRequest(requester, payload, responsesToExpectCount, this::onResponse, this::onEnd);
    }

    @When("requester sends a request with tag '$tag'")
    public void sendRequestWithTag(String tag) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload("QUERY", null);
        helper.sendRequest(requester, payload, responsesToExpectCount, this::onResponse, this::onEnd, tag);
    }

    @When("^requester sends a request with body '$body'$")
    public void sendRequestWithBody(String body) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload(null, body);
        helper.sendRequest(requester, payload, true, responsesToExpectCount, null, this::onResponse, this::onEnd);
    }

    @Given("responder server listens on fanout namespace $namespace")
    public void subscribeResponder(String namespace) {
        MsbContext context = helper.getDefaultContext();
        context.getObjectFactory().createResponderServer(namespace, new MessageTemplate(),
                (request, responderContext) -> {
                    rawIncomingMessages.add(responderContext.getOriginalMessage());
                }, String.class).listen();
    }

    private void onBeforeRequest() {
        receivedResponse = null;
        receivedResponseFuture = new CompletableFuture<>();
    }

    private void onResponse(RestPayload<Object, Object, Object, Map<String, Object>> payload) {
        if (responseProcessingDelay > 0) {
            try {
                Thread.sleep(responseProcessingDelay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (responseCountDown != null) {
            responseCountDown.countDown();
        }

        countResponsesReceived.incrementAndGet();
        receivedResponseFuture.complete(payload.getBody());
    }

    private void onEnd(Void in) {
        if (responseCountDown != null && responseCountDown.getCount() > 0) {
            fail("onEnd has been executed while not all responses were received yet, pending responses count: " + responseCountDown.getCount());
        }
    }

    @Then("requester gets response in $timeout ms")
    public void waitForResponse(long timeout) throws Exception {
        try {
            receivedResponse = receivedResponseFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeoutException) {
            fail("Response has not been received during a timeout");
        }
        Assert.assertNotNull("Response received is null", receivedResponse);
    }

    @Then("requester will get all responses in $timeout ms")
    public void waitForAllResponses(long timeout) throws Exception {
        if (!responseCountDown.await(timeout, TimeUnit.MILLISECONDS)) {
            fail("All responses has not been received during a timeout, pending count: " + responseCountDown.getCount());
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

    @Then("request forward namespace equals $forwardNamespace")
    public void responseEquals(String forwardNamespace) throws Exception {
        assertEquals(forwardNamespace, latestForwardNamespace);
    }


    @Then("message envelope topics section is $table")
    public void checkTopicsSection(ExamplesTable table) throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        Message lastMessage = rawIncomingMessages.pollLast();

        Map<String, String> expected = table.getRow(0);
        String expectedTo = normalizeNull(expected.get("to"));
        String expectedForward = normalizeNull(expected.get("forward"));
        String expectedResponse = normalizeNull(expected.get("response"));
        String expectedRoutingKey = normalizeNull(expected.get("routingKey"));

        Topics topics = lastMessage.getTopics();
        assertEquals("Invalid value of topic 'to'", expectedTo, topics.getTo());
        assertEquals("Invalid value of topic 'forward'", expectedForward, topics.getForward());
        assertEquals("Invalid value of topic 'response'", expectedResponse, topics.getResponse());
        assertEquals("Invalid value of field 'routingKey'", expectedRoutingKey, topics.getRoutingKey());
    }

    private String normalizeNull(String string) {
        if (string != null && string.trim().equalsIgnoreCase("null")) {
            return null;
        } else {
            return string;
        }
    }

    @Then("responder requests received count equals $expectedRequestsReceivedCount")
    public void requestCountEquals(int expectedCountRequestsReceived) throws Exception {
        assertEquals(expectedCountRequestsReceived, countRequestsReceived.get());
    }

    @Then("requester responses received count equals $expectedRequestsReceivedCount")
    public void responseCountEquals(int expectedCountResponsesReceived) throws Exception {
        assertEquals(expectedCountResponsesReceived, countResponsesReceived.get());
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

    @BeforeScenario
    public void resetMockResponses() {
        responses.clear();
    }
}
