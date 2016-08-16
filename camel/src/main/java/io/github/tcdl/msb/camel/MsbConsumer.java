package io.github.tcdl.msb.camel;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Msb consumer.
 */
public class MsbConsumer extends DefaultConsumer {

    private static final transient Logger LOG = LoggerFactory.getLogger(MsbConsumer.class);

    private final MsbEndpoint endpoint;
    private Config msbConfig;
    private MsbContext msbContext;

    public MsbConsumer(MsbEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    public Config getMsbConfig() {
        return msbConfig;
    }

    public MsbContext getMsbContext() {
        return msbContext;
    }

    @Override
    protected void doStart() throws Exception {

        LOG.info("Starting msb component ...");

        msbConfig = MsbConfig.from(endpoint.getEndpointConfiguration());

        msbContext = new MsbContextBuilder()
                .withConfig(msbConfig)
                .enableShutdownHook(true)
                .build();

        String namespace = endpoint.getEndpointConfiguration().getURI().getAuthority();

        msbContext.getObjectFactory().createResponderServer(namespace, new MessageTemplate(),
                (request, responderContext) -> {

                    Exchange exchange = endpoint.createExchange();
                    exchange.getIn().setBody(request);

                    try {
                        getProcessor().process(exchange);
                    } finally {
                        if (exchange.getException() != null) {
                            getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
                        }
                    }

                }, JsonNode.class)
                .listen();

        Config maskedConfig = msbConfig.withValue("msbConfig.brokerConfig.password", ConfigValueFactory.fromAnyRef("xxx"));

        LOG.info("Using configuration: {}", maskedConfig.root().render());
    }

    @Override
    protected void doStop() throws Exception {
        if (msbContext != null) {
            msbContext.shutdown();
        }
    }
}
