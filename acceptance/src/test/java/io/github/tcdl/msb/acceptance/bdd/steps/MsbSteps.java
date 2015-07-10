package io.github.tcdl.msb.acceptance.bdd.steps;

import io.github.tcdl.msb.acceptance.MsbTestHelper;
import io.github.tcdl.msb.api.MsbContext;
import org.jbehave.core.annotations.Given;

import java.lang.reflect.Method;

public class MsbSteps {

    private final static String MICROSERVICE_PACKAGE = "io.github.tcdl.msb.examples";

    MsbTestHelper helper = MsbTestHelper.getInstance();

    // microservices steps
    @Given("microservice $microservice")
    public void startMicroservice(String microserviceClassName) throws Throwable {
        Class microserviceClass = getClass().getClassLoader().loadClass(MICROSERVICE_PACKAGE + "." + microserviceClassName);
        Method startMethod = microserviceClass.getMethod("start", MsbContext.class);
        startMethod.invoke(microserviceClass.newInstance(), helper.getContext());
    }
}
