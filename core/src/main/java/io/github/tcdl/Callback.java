package io.github.tcdl;

/**
 * General purpose interface for callbacks
 *
 * @param <T> the type of parameter passed to callback during invocation
 *
 * Created by rdro on 5/28/2015.
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
