package io.github.tcdl.events;

/**
 * Created by rdro on 4/28/2015.
 */
public class TwoArgumentsAdapter<A1, A2> implements EventHandler {

    @SuppressWarnings("unchecked")
    public void onEvent(Object... args) {
        if (args.length == 2) {
            onEvent((A1) args[0], (A2) args[1]);
        } else {
            throw new IllegalArgumentException("Expecting 2 arguments but got " + args.length);
        }
    }

    public void onEvent(A1 arg1, A2 arg2) {
    }
}