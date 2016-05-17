package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link MessageHandlerInvoker} implementations that rely on a custom
 * threading model.
 */
public abstract class ExecutorBasedMessageHandlerInvoker implements MessageHandlerInvoker {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorBasedMessageHandlerInvoker.class);

    protected final ConsumerExecutorFactory consumerExecutorFactory;

    public ExecutorBasedMessageHandlerInvoker(ConsumerExecutorFactory consumerExecutorFactory) {
        this.consumerExecutorFactory = consumerExecutorFactory;
    }

    @Override
    public void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler) {
        MessageProcessingTask task = new MessageProcessingTask(messageHandler, message, acknowledgeHandler);
        doSubmitTask(task, message);
        LOG.debug("[correlation id: {}] Message has been put in the processing queue.",
                message.getCorrelationId());
    }

    protected abstract void doSubmitTask(MessageProcessingTask task, Message message);

}
