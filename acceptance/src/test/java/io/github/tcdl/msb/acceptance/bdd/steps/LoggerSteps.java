package io.github.tcdl.msb.acceptance.bdd.steps;


import io.github.tcdl.msb.acceptance.bdd.util.TestOutputStreamAppender;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;
import org.junit.Assert;

public class LoggerSteps {

    @Then("log contains '$substring'")
    public void logContains(String substring) throws Exception {
        Assert.assertTrue("String not found '" + substring + "'", TestOutputStreamAppender.isPresent(substring, 5, 500));
    }

    @Given("clear log")
    public void clearLog() throws Exception {
        TestOutputStreamAppender.reset();
    }
}
