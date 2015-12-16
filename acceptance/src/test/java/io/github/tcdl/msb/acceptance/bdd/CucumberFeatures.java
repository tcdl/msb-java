package io.github.tcdl.msb.acceptance.bdd;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        glue = {"io.github.tcdl.msb.acceptance.bdd.steps"},
        features = {"classpath:scenarios/"},
        format = {"pretty"},
        strict = true
)
public class CucumberFeatures {
}
