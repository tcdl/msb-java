package tcdl.msb.events;

/**
 * Created by rdro on 4/29/2015.
 */

public class ThreeArgumentsAdapter<A1, A2, A3> implements EventHandler {

    @SuppressWarnings("unchecked")
    public void onEvent(Object... args) {
        if (args.length == 3) {
            onEvent((A1)args[0], (A2)args[1], (A3)args[2]);
        } else {
            throw new IllegalArgumentException("Expecting 3 arguments but got " + args.length);
        }
    }

    public void onEvent(A1 arg1, A2 arg2, A3 arg3) {
    }
}