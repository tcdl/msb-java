package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.collector.ExecutionOptionsAwareMessageHandler;
import org.apache.commons.lang3.Validate;

/**
 * Created by Alexandr Zolotov
 * 23.05.16
 */
public class DirectInvocationCapableInvoker implements MessageHandlerInvoker {

    private final MessageHandlerInvoker clientMessageHandlerInvoker;
    private final DirectMessageHandlerInvoker directMessageHandlerInvoker;

    /**
     * Creates composite delegate that is guarantied to have an instance of {@link DirectMessageHandlerInvoker} in its disposal.
     * There is no need to instantiate it in client code. It is intended to be used only internally by the library.
     */
    public DirectInvocationCapableInvoker(MessageHandlerInvoker clientMessageHandlerInvoker) {
        Validate.notNull(clientMessageHandlerInvoker);
        this.clientMessageHandlerInvoker = clientMessageHandlerInvoker;
        if (clientMessageHandlerInvoker instanceof DirectMessageHandlerInvoker) {
            this.directMessageHandlerInvoker = (DirectMessageHandlerInvoker) clientMessageHandlerInvoker;
        } else {
            this.directMessageHandlerInvoker = new DirectMessageHandlerInvoker();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler) {
        if (messageHandler instanceof ExecutionOptionsAwareMessageHandler && ((ExecutionOptionsAwareMessageHandler) messageHandler).isDirectlyInvokable()) {
            directMessageHandlerInvoker.execute(messageHandler, message, acknowledgeHandler);
        } else {
            clientMessageHandlerInvoker.execute(messageHandler, message, acknowledgeHandler);
        }
    }

    @Override
    public void shutdown() {
        clientMessageHandlerInvoker.shutdown();
        directMessageHandlerInvoker.shutdown();
    }
}