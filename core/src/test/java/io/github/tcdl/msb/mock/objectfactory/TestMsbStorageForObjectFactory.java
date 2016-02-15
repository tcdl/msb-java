package io.github.tcdl.msb.mock.objectfactory;
import io.github.tcdl.msb.api.MsbContext;

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides statics-based storage and accessors for {@link TestMsbObjectFactory}-based
 * testing.
 */
public class TestMsbStorageForObjectFactory {
    private final Map<String, RequesterCapture> requesters = new HashMap<>();
    private final Map<String, ResponderCapture> responders = new HashMap<>();

    public static TestMsbStorageForObjectFactory extract(MsbContext msbContext) {
        return ((TestMsbObjectFactory)msbContext.getObjectFactory()).getStorage();
    }

    synchronized <T> void addCapture(RequesterCapture<T> requesterCapture) {
        requesters.put(requesterCapture.getNamespace(), requesterCapture);
    }

    synchronized <T> void addCapture(ResponderCapture<T> responderCapture) {
        responders.put(responderCapture.getNamespace(), responderCapture);
    }


    /**
     * Reset the storage.
     */
    public synchronized void cleanup() {
        requesters.clear();
        responders.clear();
    }

    /**
     * Get captured requester params (including handlers).
     * @param namespace
     * @param <T>
     * @return
     */
    public synchronized <T> RequesterCapture<T> getRequesterCapture(String namespace) {
        return (RequesterCapture<T>) requesters.get(namespace);
    }

    /**
     * Get captured responder params (including handlers).
     * @param namespace
     * @param <T>
     * @return
     */
    public synchronized <T> ResponderCapture<T> getResponderCapture(String namespace) {
        return (ResponderCapture<T>) responders.get(namespace);
    }
}
