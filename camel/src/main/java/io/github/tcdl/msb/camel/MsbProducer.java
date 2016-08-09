package io.github.tcdl.msb.camel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.support.Utils;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.tcdl.msb.support.Utils.ifNull;

/**
 * The Msb producer.
 */
public class MsbProducer extends DefaultProducer {

    private static final transient Logger LOG = LoggerFactory.getLogger(MsbProducer.class);

    private MsbEndpoint endpoint;
    private MsbContext msbContext;
    private ObjectMapper objectMapper;

    public MsbProducer(MsbEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.objectMapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public void process(Exchange exchange) throws Exception {

        if (msbContext == null) {
            msbContext = new MsbContextBuilder()
                    .withConfig(MsbConfig.from(endpoint.getEndpointConfiguration()))
                    .enableShutdownHook(true)
                    .build();
        }

        String namespace = endpoint.getEndpointConfiguration().getURI().getAuthority();
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withMessageTemplate(new MessageTemplate())
                .withWaitForResponses(0)
                .build();

        try {
            Object payloadObject = ifNull(exchange.getIn().getBody(), "{}");
            JsonNode payload = Utils.convert(payloadObject, JsonNode.class, objectMapper);

            msbContext
                    .getObjectFactory()
                    .createRequester(namespace, requestOptions, JsonNode.class)
                    .publish(payload);
        } catch (Exception e) {
            LOG.error("Error processing exchange", exchange, exchange.getException());
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (msbContext != null) {
            msbContext.shutdown();
        }
    }
}
