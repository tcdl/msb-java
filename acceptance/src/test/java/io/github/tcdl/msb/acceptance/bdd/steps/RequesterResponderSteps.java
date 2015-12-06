package io.github.tcdl.msb.acceptance.bdd.steps;

import static io.github.tcdl.msb.acceptance.MsbTestHelper.DEFAULT_CONTEXT_NAME;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.Utils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    private Requester<RestPayload> requester;
    private String responseBody;
    private Map<String, Object> receivedResponse;
    private CompletableFuture<Map<String, Object>> receivedResponseFuture;
    private int countRequestsReceived = 0;
    private Optional<String> nextRequestAckType = Optional.empty();


    // responder steps
    @Given("responder server listens on namespace $namespace")
    public void createResponderServer(String namespace) {
        createResponderServer(DEFAULT_CONTEXT_NAME, namespace);
    }

    @Given("responder server from $contextName listens on namespace $namespace")
    @When("responder server from $contextName listens on namespace $namespace")
    public void createResponderServer(String contextName, String namespace) {
        ObjectMapper mapper = helper.getPayloadMapper(contextName);
        countRequestsReceived = 0;
        helper.createResponderServer(contextName, namespace, (request, responderContext) -> {
            if (responseBody != null) {
                countRequestsReceived++;
                boolean isSendResponse = true;

                switch (nextRequestAckType.orElseGet(()->"auto")) {
                    case "confirm":
                        responderContext.getAcknowledgementHandler().confirmMessage();
                        break;
                    case "reject":
                        responderContext.getAcknowledgementHandler().rejectMessage();
                        isSendResponse = false;
                        break;
                }

                nextRequestAckType = Optional.empty();

                if(isSendResponse) {
                    RestPayload payload = new RestPayload.Builder<Object, Object, Object, Map>()
                            .withBody(Utils.fromJson(responseBody, Map.class, mapper))
                            .build();
                    responderContext.getResponder().send(payload);
                }
            }
        }).listen();
    }

    @Given("responder server will $nextRequestAckType next request")
    public void setNextRequestAckType(String nextRequestAckType) throws Exception {
        this.nextRequestAckType = Optional.of(nextRequestAckType);

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
        helper.sendRequest(requester, payload, 1, this::onResponse);
    }

    @When("requester sends a request with query '$query'")
    public void sendRequestWithQuery(String query) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload(query, null);
        helper.sendRequest(requester, payload, true, 1, null, this::onResponse);
    }

    @When("requester sends a request with body '$body'")
    public void sendRequestWithBody(String body) throws Exception {
        onBeforeRequest();
        RestPayload<?, ?, ?, ?> payload = helper.createFacetParserPayload(null, body);
        helper.sendRequest(requester, payload, true, 1, null, this::onResponse);
    }

    private void onBeforeRequest() {
        receivedResponse = null;
        receivedResponseFuture = new CompletableFuture<>();
    }

    private void onResponse(RestPayload<Object, Object, Object, Map<String, Object>> payload) {
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
        Assert.assertEquals(expectedCountRequestsReceived, countRequestsReceived);
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
