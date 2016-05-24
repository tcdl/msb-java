package io.github.tcdl.msb.collector;

import io.github.tcdl.msb.MessageHandler;

/**
 * Created by Alexandr Zolotov
 * 19.05.16
 */
public interface ExecutionOptionsAwareMessageHandler extends MessageHandler {

    /**
     * Indicates whether handler should be executed by main message handling thread (true if so) or by thread from
     * consumer thread pool.
     */
    boolean forceDirectInvocation();
}