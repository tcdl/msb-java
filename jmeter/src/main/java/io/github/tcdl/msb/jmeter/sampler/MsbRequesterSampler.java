package io.github.tcdl.msb.jmeter.sampler;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MsbRequesterSampler extends AbstractSampler {

    private static final Logger LOG = LoggingManager.getLoggerForClass();

    private static AtomicInteger classCount = new AtomicInteger(0);

    private String MSB_BROKER_CONFIG_ROOT = "msbConfig.brokerConfig";

    private RequesterConfig currentRequesterConfig;
    private MsbContext msbContext;

    private ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public MsbRequesterSampler() {
        classCount.incrementAndGet();
        trace("MsbRequesterSampler()");
    }

    public void init() {
        // do nothing
    }

    private void initMsb(RequesterConfig requesterConfig) {
        Config msbConfig = ConfigFactory.load()
                .withValue(MSB_BROKER_CONFIG_ROOT + ".host", ConfigValueFactory.fromAnyRef(requesterConfig.getHost()))
                .withValue(MSB_BROKER_CONFIG_ROOT + ".port", ConfigValueFactory.fromAnyRef(requesterConfig.getPort()))
                .withValue(MSB_BROKER_CONFIG_ROOT + ".virtualHost", ConfigValueFactory.fromAnyRef(requesterConfig.getVirtualHost()))
                .withValue(MSB_BROKER_CONFIG_ROOT + ".username", ConfigValueFactory.fromAnyRef(requesterConfig.getUserName()))
                .withValue(MSB_BROKER_CONFIG_ROOT + ".password", ConfigValueFactory.fromAnyRef(requesterConfig.getPassword()));

        msbContext = new MsbContextBuilder()
                .withConfig(msbConfig)
                .enableShutdownHook(true)
                .build();
    }

    public SampleResult sample(Entry e) {
        trace("sample()");

        RequesterConfig requesterConfig = (RequesterConfig)getProperty(RequesterConfig.TEST_ELEMENT_CONFIG).getObjectValue();
        if (msbContext == null || !requesterConfig.equals(currentRequesterConfig)) {
            currentRequesterConfig = requesterConfig;
            initMsb(requesterConfig);
        }

        String namespace = requesterConfig.getNamespace();
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withMessageTemplate(new MessageTemplate())
                .withResponseTimeout(requesterConfig.getTimeout())
                .withForwardNamespace(requesterConfig.getForwardNamespace())
                .withWaitForResponses(requesterConfig.getWaitForResponses() ? requesterConfig.getNumberOfResponses() : 0)
                .build();

        String payloadJson = StringUtils.isNotEmpty(requesterConfig.getRequestPayload()) ? requesterConfig.getRequestPayload() : "{}";
        JsonNode payload = Utils.fromJson(payloadJson, JsonNode.class, objectMapper);

        final CountDownLatch waitForResponse = new CountDownLatch(requesterConfig.getNumberOfResponses());
        final ArrayNode responses = objectMapper.createArrayNode();

        SampleResult res = new SampleResult();
        res.setSampleLabel(this.getName());

        res.sampleStart();

        msbContext
                .getObjectFactory()
                .createRequester(namespace, requestOptions, JsonNode.class)
                .onResponse((response, messageContext) -> {
                        waitForResponse.countDown();
                        responses.add(response);
                })
                .publish(payload);

        if (requesterConfig.getWaitForResponses()) {
            try {
                waitForResponse.await(2 * requesterConfig.getTimeout(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                res.setResponseMessage(ie.getMessage());
            }
        }

        res.sampleEnd();

        int numberOfReceivedResponses = requesterConfig.getNumberOfResponses() - (int) waitForResponse.getCount();
        trace("Received " + numberOfReceivedResponses + " responses from " + requesterConfig.getNumberOfResponses());
        boolean isResultOk = !requesterConfig.getWaitForResponses() || waitForResponse.getCount() == 0;

        if (isResultOk) {
            String responsesAsJson = responses.toString();
            trace("Responses: " + responsesAsJson);

            res.setSuccessful(true);
            res.setResponseCodeOK();
            res.setResponseMessageOK();

            res.setDataType(SampleResult.TEXT);
            res.setResponseData(responsesAsJson, null);
        } else {
            res.setSuccessful(false);
            res.setResponseCode("500");
            res.setResponseMessage("No response(s)");
        }

        return res;
    }

    private void trace(String message) {
        LOG.info(Thread.currentThread().getName() + " (" + classCount.get() + ") " + this.getName() + " " + message);
    }

    @Override
    protected void finalize() throws Throwable {
        if (msbContext != null) {
            msbContext.shutdown();
        }
    }
}