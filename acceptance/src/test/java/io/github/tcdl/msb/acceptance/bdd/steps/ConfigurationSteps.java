package io.github.tcdl.msb.acceptance.bdd.steps;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.jbehave.core.annotations.Given;

/**
 * Steps to manipulate with MSB configuration
 */
public class ConfigurationSteps extends MsbSteps {

    private String MSB_CONFIG_ROOT = "msbConfig";
    private String VALIDATE_MESSAGE = MSB_CONFIG_ROOT + ".validateMessage";
    private String TIME_THREAD_POOL_SIZE = MSB_CONFIG_ROOT + ".timerThreadPoolSize";

    private String MSB_BROKER_CONFIG_ROOT = "msbConfig.brokerConfig";
    private String MSB_BROKER_CONSUMER_THREAD_POOL_SIZE = MSB_BROKER_CONFIG_ROOT + ".consumerThreadPoolSize";
    private String MSB_BROKER_CONSUMER_THREAD_POOL_QUEUE_CAPACITY = MSB_BROKER_CONFIG_ROOT + ".consumerThreadPoolQueueCapacity";

    private Config config = ConfigFactory.load();

    @Given("MSB configuration with validate message $validate")
    public void initWithValidateMessage(boolean validate) {
        config = config.withValue(VALIDATE_MESSAGE, ConfigValueFactory.fromAnyRef(validate));
    }

    @Given("MSB configuration with timer thread pool size $size")
    public void initWithTimerThreadPoolSize(int size) {
        config = config.withValue(TIME_THREAD_POOL_SIZE, ConfigValueFactory.fromAnyRef(size));
    }

    @Given("MSB configuration with consumer thread pool size $size")
    public void initWithConsumerThreadPoolSize(int size) {
        config = config.withValue(MSB_BROKER_CONSUMER_THREAD_POOL_SIZE, ConfigValueFactory.fromAnyRef(size));
    }

    @Given("MSB configuration with consumer thread pool queue capacity $capacity")
    public void initWithConsumerThreadPoolQueueCapacity(int capacity) {
        config = config.withValue(MSB_BROKER_CONSUMER_THREAD_POOL_QUEUE_CAPACITY, ConfigValueFactory.fromAnyRef(capacity));
    }

    @Given("MSB init")
    public void initMSB() {
        helper.initWithConfig(config);
    }

    @Given("MSB shutdown")
    public void shutdownMSB() {
        helper.shutdown();
    }

}

