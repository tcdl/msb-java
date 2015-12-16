package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.adapters.MessageHandlerInvokeAdapter;
import io.github.tcdl.msb.api.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * {@link MessageHandlerInvokeAdapter} implementation that preforms a an {@link MessageHandler} invocation
 * in a separate thread.
 */
public class AmqpMessageHandlerInvokeAdapter implements MessageHandlerInvokeAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpMessageHandlerInvokeAdapter.class);

    private final ExecutorService consumerThreadPool;

    /**
     * @param consumerThreadPool thread pool to be used for {@link MessageHandler} invocations.
     */
    public AmqpMessageHandlerInvokeAdapter(ExecutorService consumerThreadPool) {
        this.consumerThreadPool = consumerThreadPool;
    }

    @Override
    public void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler) {
        consumerThreadPool.submit(new AmqpMessageProcessingTask(messageHandler, message, acknowledgeHandler));
        LOG.debug(String.format("[correlation id: %s] Message has been put in the processing queue.",
                message.getCorrelationId()));
    }
}
