package io.github.tcdl.msb.acceptance.bdd.steps;

import io.github.tcdl.msb.acceptance.bdd.util.ListAppender;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;
import org.junit.Assert;

public class LoggerSteps {

    @Then("log contains '$substring'")
    public void logContains(String substring) throws Exception {
        Assert.assertNotNull("String not found '" + substring + "'", ListAppender.getInstance().findLine(substring, 5, 500));
    }

    @Given("clear log")
    public void clearLog() throws Exception {
        ListAppender.getInstance().reset();
    }
}
