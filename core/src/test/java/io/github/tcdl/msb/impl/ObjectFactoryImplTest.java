package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

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
    public void testCreateResponderServer() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        @SuppressWarnings("unchecked")
        ResponderServer expectedResponderServer = objectFactory
                .createResponderServer(NAMESPACE, mock(MessageTemplate.class), mock(ResponderServer.RequestHandler.class));
        assertNotNull(expectedResponderServer);
    }

    @Test
    public void getPayloadConverter() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        PayloadConverter payloadConverter = objectFactory.getPayloadConverter();
        assertNotNull(payloadConverter);
    }

    @Test
    public void testCreateFireAndForgetRequester() throws Exception {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        Requester expectedRequester = objectFactory.createRequesterForFireAndForget(NAMESPACE);
        assertNotNull(expectedRequester);
    }
}
