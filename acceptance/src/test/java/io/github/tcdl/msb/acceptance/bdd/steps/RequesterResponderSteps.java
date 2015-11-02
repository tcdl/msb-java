package io.github.tcdl.msb.acceptance.bdd.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.acceptance.MsbTestHelper;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.Utils;
import org.hamcrest.Matchers;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;
import org.jbehave.core.model.ExamplesTable;
import org.jbehave.core.model.OutcomesTable;
import org.junit.Assert;

import java.util.Map;

/**
 * Steps to send requests and respond with predifined responses
 */
public class RequesterResponderSteps extends MsbSteps {

    private Requester<RestPayload<Object, Object, Object, Map<String, Object>>> requester;
    private String responseBody;
    private Map<String, Object> receivedResponse;

    // responder steps
    @Given("responder server listens on namespace $namespace")
    public void createResponderServer(String namespace) {
        createResponderServer(MsbTestHelper.DEFAULT_CONTEXT_NAME, namespace);
    }

    @Given("responder server from $contextName listens on namespace $namespace")
    @When("responder server from $contextName listens on namespace $namespace")
    public void createResponderServer(String contextName, String namespace) {
        ObjectMapper mapper = helper.getPayloadMapper(contextName);
        helper.createResponderServer(contextName, namespace, (request, responder) -> {
            if (responseBody != null) {
                RestPayload payload = new RestPayload.Builder<Object, Object, Object, Map>()
                        .withBody(Utils.fromJson(responseBody, Map.class, mapper))
                        .build();
                responder.send(payload);
            }
        }).listen();
    }

    @Given("responder server responds with '$body'")
    @When("responder server responds with '$body'")
    public void respond(String body) {
        responseBody = body;
    }

    // requester steps
    @Given("requester sends requests to namespace $namespace")
    public void createRequester(String namespace) {
        createRequester(MsbTestHelper.DEFAULT_CONTEXT_NAME, namespace);
    }

    @Given("requester from $contextName sends requests to namespace $namespace")
    public void createRequester(String contextName, String namespace) {
        requester = helper.createRequester(contextName, namespace, 1);
    }

    @When("requester sends a request")
    public void sendRequest() throws Exception {
        sendRequest(MsbTestHelper.DEFAULT_CONTEXT_NAME);
    }

    @When("requester from $contextName sends a request")
    public void sendRequest(String contextName) throws Exception {
        helper.sendRequest(contextName, requester, 1, this::onResponse);
    }

    @When("requester sends a request with query '$query'")
    public void sendRequestWithQuery(String query) throws Exception {
        helper.sendRequest(requester, query, null, true, 1, null, this::onResponse);
    }

    @When("requester sends a request with body '$body'")
    public void sendRequestWithBody(String body) throws Exception {
        helper.sendRequest(requester, null, body, true, 1, null, this::onResponse);
    }

    private void onResponse(RestPayload<Object, Object, Object, Map<String, Object>> payload) {
        receivedResponse = payload.getBody();
    }

    @Then("requester gets response in $timeout ms")
    public void waitForResponse(long timeout) throws Exception {
        Thread.sleep(timeout);
        Assert.assertNotNull("Response has not been received", receivedResponse);
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
