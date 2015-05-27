package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.middleware.MiddlewareChain;
import io.github.tcdl.support.TestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by rdro on 4/30/2015.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ResponderServer.class, Responder.class } )
public class ResponderServerTest {

    @Test
    public void testResponderServerProcessSuccess() throws Exception {
        Config config = ConfigFactory.load();
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        ChannelManager channelManager = new ChannelManager(msbConfig);
        MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails());

        Middleware middleware = (request, responder) -> {};

        ResponderServer
                .create(TestUtils.createSimpleConfig(), channelManager, messageFactory, msbConfig)
                .use(middleware)
                .listen();

        mockStatic(CompletableFuture.class);
        ArgumentCaptor<Supplier> middlewareCaptor = ArgumentCaptor.forClass(Supplier.class);

        channelManager.emit(Event.MESSAGE_EVENT, TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("test:responser-server"));

        verifyStatic();
        CompletableFuture.supplyAsync(middlewareCaptor.capture());
        MiddlewareChain middlewareChain = (MiddlewareChain) middlewareCaptor.getValue().get();

        assertNotNull(middlewareChain);
        assertEquals(middleware, middlewareChain.getMiddleware().iterator().next());
    }
}
