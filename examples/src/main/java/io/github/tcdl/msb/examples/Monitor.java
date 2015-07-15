package io.github.tcdl.msb.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor {
    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);

    public static void main(String... args) {
        MsbContext msbContext = new MsbContextBuilder()
                .enableChannelMonitorAgent(false)
                .enableShutdownHook(true)
                .build();

        ObjectMapper aggregatorStatsMapper = new ObjectMapper();
        aggregatorStatsMapper.registerModule(new JSR310Module());
        aggregatorStatsMapper.enable(SerializationFeature.INDENT_OUTPUT);

        ChannelMonitorAggregator channelMonitorAggregator = msbContext.getObjectFactory().createChannelMonitorAggregator(arg -> LOG.info("Received monitoring info " + Utils.toJson(arg, aggregatorStatsMapper)));
        channelMonitorAggregator.start();
    }
}
