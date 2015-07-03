package io.github.tcdl.examples;

import io.github.tcdl.api.Requester;
import io.github.tcdl.api.message.payload.Payload;
import io.github.tcdl.support.Utils;
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

    private Requester requester;
    private String responseBody;
    private Map<String, Object> receivedResponse;

    // responder steps
    @Given("responder server listens on namespace $namespace")
    public void createResponderServer(String namespace) {
        helper.createResponderServer(namespace, (request, responder) -> {
            if (responseBody != null) {
                Payload payload = new Payload.PayloadBuilder().withBody(Utils.fromJson(responseBody, Map.class)).build();
                responder.send(payload);
            }
        })
        .listen();
    }

    @Given("responder server responds with '$body'")
    public void respond(String body) {
        responseBody = body;
    }

    // requester steps
    @Given("requester sends requests to namespace $namespace")
    public void createRequester(String namespace) {
        requester = helper.createRequester(namespace, 1);
    }

    @When("requester sends a request")
    public void sendRequest() throws Exception {
        helper.sendRequest(requester, 1, this::onResponse);
    }

    @When("requester sends a request with query '$query'")
    public void sendRequestWithQuery(String query) throws Exception {
        helper.sendRequest(requester, query, null, true, 1, null, this::onResponse);
    }

    @When("requester sends a request with body '$body'")
    public void sendRequestWithBody(String body) throws Exception {
        helper.sendRequest(requester, null, body, true, 1, null, this::onResponse);
    }

    private void onResponse(Payload payload) {
        if (payload.getBody() != null) {
            receivedResponse = payload.getBodyAs(Map.class);
        }
    }

    @Then("requester gets response in $timeout ms")
    public void waitForResponse(long timeout) throws Exception {
        Thread.sleep(timeout);
        Assert.assertNotNull("Response has not been received", receivedResponse);
    }

    @Then("response equals $table")
    public void responseEquals(ExamplesTable table) throws Exception {
        Map<String,String> expected = table.getRow(0);
        OutcomesTable outcomes = new OutcomesTable();

        for (String key : expected.keySet()){
            outcomes.addOutcome(key, receivedResponse.get(key), Matchers.equalTo(expected.get(key)));
        }

        outcomes.verify();
    }

    @Then("response contains $table")
    public void responseContains(ExamplesTable table) throws Exception {
        Map<String,String> expected = table.getRow(0);
        OutcomesTable outcomes = new OutcomesTable();

        for (String key : expected.keySet()){
            outcomes.addOutcome(key, receivedResponse.get(key).toString(), Matchers.containsString(expected.get(key)));
        }

        outcomes.verify();
    }
}
