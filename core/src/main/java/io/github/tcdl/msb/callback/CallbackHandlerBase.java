package io.github.tcdl.msb.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class CallbackHandlerBase implements CallbackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CallbackHandlerBase.class);

    protected final List<Runnable> callbacks = new ArrayList<>();

    public void runCallbacks() {
        for(Runnable callback: callbacks) {
            try {
                callback.run();
            } catch (Exception ex) {
                LOG.warn("Exception while trying to invoke callback", ex);
            }
        }
    }

}
