package io.github.tcdl.msb.impl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ObjectFactoryImplTest {

    private static final String NAMESPACE = "test:object-factory";

    @Mock
    private RequestOptions requestOptionsMock;

    @Test
    public void testCreateRequester() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        Requester expectedRequester = objectFactory.createRequester(NAMESPACE, mock(RequestOptions.class));
        assertNotNull(expectedRequester);
    }

    @Test
    public void testCreateRequesterWithOriginalMessage() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        Requester expectedRequester = objectFactory
                .createRequester(NAMESPACE, mock(RequestOptions.class), TestUtils.createMsbRequestMessageNoPayload("test:object-factory-incoming"));
        assertNotNull(expectedRequester);
    }

    @Test
    public void testCreateResponderServer() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        ResponderServer expectedResponderServer = objectFactory
                .createResponderServer(NAMESPACE, mock(MessageTemplate.class), mock(ResponderServer.RequestHandler.class));
        assertNotNull(expectedResponderServer);
    }
}
