package io.github.tcdl.examples;

import io.github.tcdl.api.MsbContext;
import org.jbehave.core.annotations.Given;

import java.lang.reflect.Method;

public class MsbSteps {

    private final static String MICROSERVICE_PACKAGE = "io.github.tcdl.examples";

    MsbTestHelper helper = MsbTestHelper.getInstance();

    // microservices steps
    @Given("microservice $microservice")
    public void startMicroservice(String microserviceClassName) throws Throwable {
        Class microserviceClass = getClass().getClassLoader().loadClass(MICROSERVICE_PACKAGE + "." + microserviceClassName);
        Method startMethod = microserviceClass.getMethod("start", MsbContext.class);
        startMethod.invoke(microserviceClass.newInstance(), helper.getContext());
    }
}
