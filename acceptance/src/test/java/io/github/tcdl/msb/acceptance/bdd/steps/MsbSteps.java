package io.github.tcdl.msb.acceptance.bdd.steps;

import io.github.tcdl.msb.acceptance.MsbTestHelper;
import io.github.tcdl.msb.api.MsbContext;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class MsbSteps {

    private final static String DEFAULT_PACKAGE = "io.github.tcdl.msb.examples.";

    MsbTestHelper helper = MsbTestHelper.getInstance();
    private Map<String, Object> microserviceMap = new HashMap<>();

    @Given("microservice $microservice")
    public void startMicroservice(String microservice) throws Throwable {
        Class<?> microserviceClass = getClass().getClassLoader().loadClass(resolveClass(microservice));
        Method startMethod = microserviceClass.getMethod("start", MsbContext.class);
        startMethod.invoke(microserviceClass.newInstance(), helper.getDefaultContext());
    }

    @Given("start microservice $microservice with context $contextName")
    public void startMicroserviceWithContext(String microservice, String contextName) throws Throwable {
        Class<?> microserviceClass = getClass().getClassLoader().loadClass(resolveClass(microservice));
        Method startMethod = microserviceClass.getMethod("start", MsbContext.class);
        Object microserviceInstance = microserviceClass.newInstance();
        startMethod.invoke(microserviceInstance, helper.getContext(contextName));
        microserviceMap.putIfAbsent(microservice, microserviceInstance);
    }

    @Then("stop microservice $microservice")
    public void stopMicroservice(String microservice) throws Throwable {
        if (microserviceMap.containsKey(microservice)) {
            Object microserviceInstance = microserviceMap.get(microservice);
            Class<?> microserviceClass = getClass().getClassLoader().loadClass(resolveClass(microservice));
            Method stopMethod = microserviceClass.getMethod("stop");
            stopMethod.invoke(microserviceInstance);
        }
    }

    protected String resolveClass(String microservice) {
        if (microservice.startsWith("com.") || microservice.startsWith("io.")) {
            return microservice;
        } else {
            return DEFAULT_PACKAGE + microservice;
        }
    }
}
