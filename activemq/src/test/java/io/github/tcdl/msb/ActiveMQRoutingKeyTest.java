
package io.github.tcdl.msb;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.api.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ActiveMQRoutingKeyTest {

    private String namespace = "activemq:rounting-keys:test";
    private List<MsbContext> msbContexts;
    private List<ResponderServer> responderServers;

    @Before
    public void setUp() {
        msbContexts = new LinkedList<>();
        responderServers = new LinkedList<>();
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
    public void requestResponseWithRoutingKeyTest() throws Exception {
        final String message = "test message";
        final String routingKey = "RK";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withRoutingKey(routingKey)
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .withBindingKeys(Sets.newHashSet(routingKey))
                .build();

        MsbContext msbContext = new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build();
        msbContexts.add(msbContext);

        ResponderServer.RequestHandler<String> handlerMock = mock(ResponderServer.RequestHandler.class);

        responderServers.add(msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                handlerMock, String.class).listen());

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        verify(handlerMock, timeout(5000).times(1)).process(eq(message), any(ResponderContext.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void requestResponseWithDifferentRoutingKeyTest() throws Exception {
        final String message = "test message";
        final String routingKey1 = "RK1";
        final String routingKey2 = "RK2";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withRoutingKey(routingKey1)
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions1 = new ActiveMQResponderOptions.Builder()
                .withBindingKeys(Sets.newHashSet(routingKey1))
                .build();

        ResponderOptions responderOptions2 = new ActiveMQResponderOptions.Builder()
                .withBindingKeys(Sets.newHashSet(routingKey2))
                .build();

        MsbContext msbContext1 = new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build();
        msbContexts.add(msbContext1);

        MsbContext msbContext2 = new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build();
        msbContexts.add(msbContext2);

        ResponderServer.RequestHandler<String> handlerMock = mock(ResponderServer.RequestHandler.class);

        responderServers.add(msbContext1.getObjectFactory().createResponderServer(namespace, responderOptions1,
                handlerMock, String.class).listen());

        responderServers.add(msbContext2.getObjectFactory().createResponderServer(namespace, responderOptions2,
                handlerMock, String.class).listen());

        msbContext1.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        verify(handlerMock, timeout(5000).times(1)).process(eq(message), any(ResponderContext.class));
    }
}
