package io.github.tcdl.msb.api;

/**
 * General purpose interface for callbacks.
 *
 * @param <T> the type of parameter passed to callback during invocation.
 */
@FunctionalInterface
public interface Callback<T> {

    /**
     * Execute callback.
     *
     * @param arg
     */
    void call(T arg);

}
