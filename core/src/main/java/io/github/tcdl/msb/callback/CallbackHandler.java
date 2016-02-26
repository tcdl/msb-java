package io.github.tcdl.msb.callback;

/**
 * Container for {@link Runnable} callbacks.
 */
public interface CallbackHandler {

    /**
     * Run call callbacks within the container, catch and log expections if any.
     */
    void runCallbacks();

}
