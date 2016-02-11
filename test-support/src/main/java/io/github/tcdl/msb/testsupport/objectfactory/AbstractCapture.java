package io.github.tcdl.msb.testsupport.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Abstract capture class for requesters and responders.
 * @param <T>
 */
public abstract class AbstractCapture<T> {
    private final String namespace;
    private final TypeReference<T> payloadTypeReference;
    private final Class<T> payloadClass;

    public AbstractCapture(String namespace,
            TypeReference<T> payloadTypeReference, Class<T> payloadClass) {
        this.namespace = namespace;
        this.payloadTypeReference = payloadTypeReference;
        this.payloadClass = payloadClass;
    }

    public String getNamespace() {
        return namespace;
    }

    public TypeReference<T> getPayloadTypeReference() {
        return payloadTypeReference;
    }

    public Class<T> getPayloadClass() {
        return payloadClass;
    }


}