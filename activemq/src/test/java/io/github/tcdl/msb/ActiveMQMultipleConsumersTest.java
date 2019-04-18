package io.github.tcdl.msb;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import io.github.tcdl.msb.api.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ActiveMQMultipleConsumersTest {

    private String namespace = "activemq:multiple-consumers:test";
    private List<MsbContext> msbContexts;
    private List<ResponderServer> responderServers;

    @Before
    public void setUp() {
        responderServers = new LinkedList<>();
        msbContexts = new LinkedList<>();
        msbContexts.add(new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build());
    }

    @After
    public void tearDown() throws InterruptedException {
        responderServers.forEach(ResponderServer::stop);
        responderServers.clear();
        msbContexts.forEach(MsbContext::shutdown);
        msbContexts.clear();
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleConsumersTest()  throws Exception {
        final int numberOfConsumers = 3;
        final String message = "test message";

        MsbContext msbContext = msbContexts.get(0);

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .build();

        ResponderServer.RequestHandler<String> handlerMock = mock(ResponderServer.RequestHandler.class);
        IntStream.range(0, numberOfConsumers).forEach((i) -> {
            Config config = getConfigWith(ConfigFactory.load(), "msbConfig.brokerConfig.groupId", "consumer" + (i+1));

            MsbContext consumerMsbContext = new MsbContextBuilder()
                    .enableShutdownHook(true)
                    .withConfig(config)
                    .build();

            msbContexts.add(consumerMsbContext);

            responderServers.add(consumerMsbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                    handlerMock, String.class).listen());
        });

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        verify(handlerMock, timeout(5000).times(numberOfConsumers)).process(eq(message), any(ResponderContext.class));
    }

    private Config getConfigWith(Config config, String path, Object value) {
        ConfigValue configValue = ConfigFactory.parseString(path + "=\"" + value + "\"").getValue(path);
        return  config.withValue(path, configValue);
    }
}
