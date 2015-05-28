package io.github.tcdl;

/**
 * General purpose interface for callbacks
 *
 * Created by rdro on 5/28/2015.
 */
@FunctionalInterface
public interface Callback<T> {

    void call(T arg);

}
