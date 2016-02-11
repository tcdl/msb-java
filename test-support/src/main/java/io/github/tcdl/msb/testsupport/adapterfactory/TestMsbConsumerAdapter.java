package io.github.tcdl.msb.testsupport.adapterfactory;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.adapters.ConsumerAdapter;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class TestMsbConsumerAdapter implements ConsumerAdapter {

    final Set<RawMessageHandler> rawMessageHandlers = new HashSet<>();

    private final String namespace;

    public TestMsbConsumerAdapter(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public void subscribe(RawMessageHandler onMessageHandler) {
        rawMessageHandlers.add(onMessageHandler);
    }

    @Override
    public void unsubscribe() {

    }

    public void pushTestMessage(String jsonMessage) {
        AcknowledgementHandlerInternal ackHandler = mock(AcknowledgementHandlerInternal.class);
        rawMessageHandlers.forEach((handler)-> handler.onMessage(jsonMessage, ackHandler));
    }
}
