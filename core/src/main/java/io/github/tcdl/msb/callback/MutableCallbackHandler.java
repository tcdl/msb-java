package io.github.tcdl.msb.callback;

/**
 * Mutable {@link CallbackHandler} implementation.
 */
public class MutableCallbackHandler extends CallbackHandlerBase {

    /**
     * Add a callback.
     * @param runnable
     */
    public void add(Runnable runnable) {
        callbacks.add(runnable);
    }

    /**
     * Remove a callback.
     * @param runnable
     */
    public void remove(Runnable runnable) {
        callbacks.remove(runnable);
    }
}
