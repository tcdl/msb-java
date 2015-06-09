package io.github.tcdl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by ruslan on 03.06.15.
 */
public class TimerProvider {

    public static final Logger LOG = LoggerFactory.getLogger(TimerProvider.class);

    private TimerTask responseTimeout;
    private TimerTask ackTimeout;
    private Collector collector;

    public TimerProvider(Collector collector) {
        this.collector = collector;
    }

    protected void enableResponseTimeout(int timeoutMs) {
        LOG.debug("Enabling response timeout for {} ms", timeoutMs);
        clearTimeout(this.responseTimeout);

        this.responseTimeout = new TimerTask() {
            @Override
            public void run() {
                LOG.debug("Response timeout expired.");
                collector.end();
            }
        };

        setTimeout(this.responseTimeout, timeoutMs);
    }

    protected void enableAckTimeout(long timeoutMs) {
        LOG.debug("Enabling ack timeout for {} ms", timeoutMs);
        if (this.ackTimeout != null) {
            LOG.debug("Ack timeout is already scheduled");
            return;
        }

        if (timeoutMs <= 0) {
            LOG.debug("Unable to schedule timeout with negative delay : {}", timeoutMs);
            return;
        }

        ackTimeout = new TimerTask() {
            @Override
            public void run() {
                if (collector.isAwaitingResponses()) {
                    LOG.debug("Ack timeout expired, but waiting for responses.");
                    return;
                }
                LOG.debug("Ack timeout expired.");
                collector.end();
            }
        };

        setTimeout(ackTimeout, timeoutMs);
    }

    public void stopTimers() {
        clearTimeout(this.responseTimeout);
        clearTimeout(this.ackTimeout);
    }

    private void setTimeout(TimerTask onTimeout, long delay) {
        new Timer().schedule(onTimeout, delay);
    }

    private void clearTimeout(TimerTask onTimeout) {
        if (onTimeout != null) {
            onTimeout.cancel();
        }
    }

}
