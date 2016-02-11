package io.github.tcdl.msb.testsupport.objectfactory;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides statics-based storage and accessors for {@link TestMsbObjectFactory}-based
 * testing.
 */
public class TestMsbStorageForObjectFactory {
    private static final Map<String, RequesterCapture> REQUESTERS = new HashMap<>();
    private static final Map<String, ResponderCapture> RESPONDERS = new HashMap<>();

    static class Internal {
        static <T> void addCapture(RequesterCapture<T> requesterCapture) {
            REQUESTERS.put(requesterCapture.getNamespace(), requesterCapture);
        }

        static <T> void addCapture(ResponderCapture<T> responderCapture) {
            RESPONDERS.put(responderCapture.getNamespace(), responderCapture);
        }
    }

    /**
     * Reset the storage.
     */
    public static void cleanup() {
        REQUESTERS.clear();
        RESPONDERS.clear();
    }

    /**
     * Get captured requester params (including handlers).
     * @param namespace
     * @param <T>
     * @return
     */
    public static <T> RequesterCapture<T> getRequesterCapture(String namespace) {
        return (RequesterCapture<T>)REQUESTERS.get(namespace);
    }

    /**
     * Get captured responder params (including handlers).
     * @param namespace
     * @param <T>
     * @return
     */
    public static <T> ResponderCapture<T> getResponderCapture(String namespace) {
        return (ResponderCapture<T>)RESPONDERS.get(namespace);
    }
}
