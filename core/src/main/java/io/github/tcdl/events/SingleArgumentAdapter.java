package io.github.tcdl.events;

/**
 * Created by rdro on 4/24/2015.
 */
public class SingleArgumentAdapter<A> implements EventHandler {

    @SuppressWarnings("unchecked")
    public void onEvent(Object... args) {
        if (args.length == 1) {
            onEvent((A) args[0]);
        } else {
            throw new IllegalArgumentException("Expecting 1 argument but got " + args.length);
        }
    }

    public void onEvent(A arg) {
    }
}
