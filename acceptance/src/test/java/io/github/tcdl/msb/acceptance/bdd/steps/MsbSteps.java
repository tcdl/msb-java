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

    protected final MsbTestHelper helper = MsbTestHelper.getInstance();
    private final Map<String, Object> microserviceMap = new HashMap<>();

    @Given("microservice $microservice")
    public synchronized void startMicroservice(String microservice) throws Throwable {
        startMicroserviceInternal(microservice, helper.getDefaultContext());
    }

    @Given("start microservice $microservice with context $contextName")
    public synchronized void startMicroserviceWithContext(String microservice, String contextName) throws Throwable {
        startMicroserviceInternal(microservice, helper.getContext(contextName));
    }

    private void startMicroserviceInternal(String microservice, MsbContext context) throws Exception {
        Class<?> microserviceClass = getClass().getClassLoader().loadClass(resolveClass(microservice));
        Method startMethod = microserviceClass.getMethod("start", MsbContext.class);
        Object microserviceInstance = microserviceClass.newInstance();
        startMethod.invoke(microserviceInstance, context);
        microserviceMap.putIfAbsent(microservice, microserviceInstance);
    }

    @Then("stop microservice $microservice")
    public synchronized void stopMicroservice(String microservice) throws Throwable {
        if (microserviceMap.containsKey(microservice)) {
            Object microserviceInstance = microserviceMap.get(microservice);
            Class<?> microserviceClass = getClass().getClassLoader().loadClass(resolveClass(microservice));
            Method stopMethod = microserviceClass.getMethod("stop");
            stopMethod.invoke(microserviceInstance);
            microserviceMap.remove(microservice);
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
