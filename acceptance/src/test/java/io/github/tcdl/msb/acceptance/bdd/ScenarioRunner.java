package io.github.tcdl.msb.acceptance.bdd;

import io.github.tcdl.msb.acceptance.bdd.steps.ConfigurationSteps;
import io.github.tcdl.msb.acceptance.bdd.steps.LoggerSteps;
import io.github.tcdl.msb.acceptance.bdd.steps.RequesterResponderSteps;
import org.jbehave.core.configuration.Configuration;
import org.jbehave.core.configuration.MostUsefulConfiguration;
import org.jbehave.core.io.LoadFromURL;
import org.jbehave.core.io.StoryFinder;
import org.jbehave.core.junit.JUnitStories;
import org.jbehave.core.reporters.Format;
import org.jbehave.core.reporters.StoryReporterBuilder;
import org.jbehave.core.steps.InjectableStepsFactory;
import org.jbehave.core.steps.InstanceStepsFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ScenarioRunner extends JUnitStories {

    private final String STORY_PATH = "target/test-classes";
    private final String STORY_PATTERN = "**/*.story";

    public List<Object> getStepInstances() {
        List<Object> steps = new ArrayList<>();
        steps.addAll(Arrays.asList(
                new ConfigurationSteps(),
                new RequesterResponderSteps(),
                new LoggerSteps()
        ));
        return steps;
    }

    @Override
    public Configuration configuration() {
        return new MostUsefulConfiguration()
                .useStoryLoader(new LoadFromURL())
                .useStoryReporterBuilder(new StoryReporterBuilder()
                        .withDefaultFormats().withFormats(Format.CONSOLE, Format.HTML)
                        .withFailureTrace(true));
    }

    @Override
    protected List<String> storyPaths() {
        return new StoryFinder().findPaths(STORY_PATH, Arrays.asList(STORY_PATTERN), Arrays.asList(""), getClass().getResource("/").toString());
    }

    @Override
    public InjectableStepsFactory stepsFactory() {
        return new InstanceStepsFactory(configuration(), getStepInstances());
    }
}
