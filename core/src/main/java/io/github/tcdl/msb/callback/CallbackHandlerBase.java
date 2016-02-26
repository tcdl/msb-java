package io.github.tcdl.msb.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public abstract class CallbackHandlerBase implements CallbackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CallbackHandlerBase.class);

    protected final Set<Runnable> callbacks = new HashSet<>();

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
