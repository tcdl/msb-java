package io.github.tcdl.msb.camel;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;

/**
 * Represents a Msb endpoint.
 */
public class MsbEndpoint extends DefaultEndpoint {

    public MsbEndpoint() {
    }

    public MsbEndpoint(String uri, MsbComponent component) {
        super(uri, component);
    }

    public MsbEndpoint(String endpointUri) {
        super(endpointUri);
    }

    public Producer createProducer() throws Exception {
        return new MsbProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new MsbConsumer(this, processor);
    }

    public boolean isSingleton() {
        return true;
    }

    @Override
    public boolean isLenientProperties() {
        return true;
    }
}
