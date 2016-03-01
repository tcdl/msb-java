package io.github.tcdl.msb.callback;

/**
 * Container for {@link Runnable} callbacks.
 */
public interface CallbackHandler {

    /**
     * Run all callbacks within the container, catch and log exceptions if any.
     */
    void runCallbacks();

}
