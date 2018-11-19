package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import org.apache.commons.lang3.RandomUtils;

import java.util.List;

/**
 * This {@link MessageHandlerInvoker} implementation delegates execution of {@link io.github.tcdl.msb.MessageHandler}
 * to one of the provided invokers. Messages with the same group (resolved by {@link MessageGroupStrategy} provided)
 * will be processed by the same invoker.
 *
 * For example, this class can be used to process messages from the same group sequentially by providing a list of
 * single-threaded invokers.
 */
public class GroupedMessageHandlerInvoker<T extends MessageHandlerInvoker> implements MessageHandlerInvoker {

    private final MessageGroupStrategy messageGroupStrategy;
    private final int numberOfInvokers;
    private final List<T> invokers;

    public GroupedMessageHandlerInvoker(List<T> invokers, MessageGroupStrategy messageGroupStrategy) {
        this.messageGroupStrategy = messageGroupStrategy;
        this.numberOfInvokers = invokers.size();
        this.invokers = invokers;
    }

    private int getInvokerKey(Message message) {
        return messageGroupStrategy.getMessageGroupId(message)
                .map(integer -> Math.abs(integer % numberOfInvokers))
                .orElseGet(() -> RandomUtils.nextInt(0, numberOfInvokers));
    }

    @Override
    public void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler) {
        int invokerKey = getInvokerKey(message);
        invokers.get(invokerKey).execute(messageHandler, message, acknowledgeHandler);
    }

    @Override
    public void shutdown() {
        invokers.forEach(MessageHandlerInvoker::shutdown);
    }
}
