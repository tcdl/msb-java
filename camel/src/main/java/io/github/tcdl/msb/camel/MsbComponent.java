package io.github.tcdl.msb.camel;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

import java.util.Map;

/**
 * Represents the component that manages {@link MsbEndpoint}.
 */
public class MsbComponent extends DefaultComponent {

    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        Endpoint endpoint = new MsbEndpoint(uri, this);
        setProperties(endpoint, parameters);
        return endpoint;
    }
}
