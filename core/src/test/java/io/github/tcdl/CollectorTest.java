package io.github.tcdl;

import static io.github.tcdl.support.TestUtils._g;
import static io.github.tcdl.support.TestUtils._m;
import static io.github.tcdl.support.TestUtils._s;
import static io.github.tcdl.support.TestUtils.createSimpleConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.support.TestUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import javax.xml.ws.Holder;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by rdro on 4/27/2015.
 */
public class CollectorTest {

    private MsbMessageOptions config;
    private MessageFactory messageFactory;

    @Before
    public void setUp() {
        this.config = createSimpleConfig();
        this.messageFactory = MessageFactory.getInstance();
    }

    @Test
    public void test_init_without_config() {
        Collector collector = new Collector(null);
        Long startedAt = _g(collector, "startedAt");

        assertNotNull(startedAt);
        assertTrue(System.currentTimeMillis() - startedAt < 10);
        assertEquals(Integer.valueOf(3000), _g(collector, "timeoutMs"));
    }

    @Test
    public void test_init_with_config() {
        config.setResponseTimeout(555);
        config.setAckTimeout(50);
        config.setWaitForResponses(1);
        Collector collector = new Collector(config);

        Long startedAt = _g(collector, "startedAt");

        assertNotNull(startedAt);
        assertTrue(System.currentTimeMillis() - startedAt < 20);
        assertTrue((Long) _g(collector, "waitForAcksUntil") - config.getAckTimeout() - System.currentTimeMillis() < 10);
        assertEquals(Integer.valueOf(555), _g(collector, "timeoutMs"));
    }

    @Test
    public void test_isAwaitingAcks() {
        Collector collector = new Collector(config);
        assertFalse(collector.isAwaitingAcks());

        _s(collector, "waitForAcksUntil", System.currentTimeMillis());
        assertFalse(collector.isAwaitingAcks());

        _s(collector, "waitForAcksUntil", System.currentTimeMillis() + 1000);
        assertTrue(collector.isAwaitingAcks());
    }

    @Test
    public void test_getMaxTimeoutMs() {
        Collector collector = new Collector(config);
        // should return base timeout
        // assertEquals(_g(collector, "timeoutMs"), _m(collector,
        // "getMaxTimeoutMs"));

        // should return max timeout
        _s(collector, "timeoutMs", 0);
        Map<String, Integer> timeoutMsById = new HashMap<String, Integer>();
        timeoutMsById.put("a", 100);
        timeoutMsById.put("b", 500);
        timeoutMsById.put("c", 1000);
        _s(collector, "timeoutMsById", timeoutMsById);

        // should return max with remaining > 0
        Map<String, Integer> responsesRemainingById = new HashMap<String, Integer>();
        responsesRemainingById.put("a", 0);
        responsesRemainingById.put("b", 1);
        responsesRemainingById.put("c", 0);
        _s(collector, "responsesRemainingById", responsesRemainingById);

        assertEquals(Integer.valueOf(500), _m(collector, "getMaxTimeoutMs"));
    }

    @Test
    public void test_getResponsesRemaining() {

        Collector collector = new Collector(config);
        assertEquals(Integer.valueOf(0), _m(collector, "getResponsesRemaining"));

        // should return sum of all responses remaining
        _s(collector, "responsesRemaining", 0);
        Map<String, Integer> responsesRemainingById = new HashMap<String, Integer>();
        responsesRemainingById.put("a", 1);
        responsesRemainingById.put("b", 2);
        responsesRemainingById.put("c", 3);
        _s(collector, "responsesRemainingById", responsesRemainingById);
        assertEquals(Integer.valueOf(6), _m(collector, "getResponsesRemaining"));
    }

    @Test
    public void test_setTimeoutMsForResponderId() {
        Collector collector = new Collector(config);

        Map<String, Integer> responsesRemainingById = new HashMap<String, Integer>();
        responsesRemainingById.put("a", 1);
        _s(collector, "responsesRemainingById", responsesRemainingById);
        _m(collector, "setTimeoutMsForResponderId", new Object[] { "a", 10000 }, String.class, Integer.class);

        assertEquals(Integer.valueOf(10000), _m(collector, "getMaxTimeoutMs"));
    }

    @Test
    public void test_setResponsesRemainingForResponderId() {
        Collector collector = new Collector(config);

        _s(collector, "responsesRemaining", 0);
        _m(collector, "setResponsesRemainingForResponderId", new Object[] { "a", 1 }, String.class, Integer.class);

        assertEquals(Integer.valueOf(1), _m(collector, "getResponsesRemaining"));

        // should return sum of all remaining responses
        _m(collector, "setResponsesRemainingForResponderId", new Object[] { "a", 2 }, String.class, Integer.class);
        assertEquals(Integer.valueOf(3), _m(collector, "getResponsesRemaining"));
    }

    @Test
    public void test_incResponsesRemaining() {
        Collector collector = new Collector(config);

        // should add to responses remaining
        _s(collector, "responsesRemaining", 0);
        assertEquals(Integer.valueOf(1), _m(collector, "incResponsesRemaining", new Object[] { 1 }, Integer.class));
        assertEquals(Integer.valueOf(3), _m(collector, "incResponsesRemaining", new Object[] { 2 }, Integer.class));
    }

    @Test
    public void test_processAck() {
        Collector collector = new Collector(config);
        _s(collector, "responsesRemaining", 0);

        // for null do nothing
        _m(collector, "processAck", new Object[] { null }, Acknowledge.class);

        // take max timeout if there no responses by id
        Acknowledge acknowledge = new Acknowledge.AcknowledgeBuilder().setResponderId("a").setTimeoutMs(1500).build();

        _m(collector, "processAck", new Object[] { acknowledge }, Acknowledge.class);
        assertEquals(Integer.valueOf(3000), _g(collector, "currentTimeoutMs"));

        // take timeout by responder id
        acknowledge = new Acknowledge.AcknowledgeBuilder().setResponderId("a").setTimeoutMs(5000).build();

        _m(collector, "setResponsesRemainingForResponderId", new Object[] { "a", 1 }, String.class, Integer.class);
        _m(collector, "processAck", new Object[] { acknowledge }, Acknowledge.class);
        assertEquals(Integer.valueOf(5000), _g(collector, "currentTimeoutMs"));
    }

    @Test
    public void test_listenForResponses() {
        String topic = config.getNamespace();
        Predicate<Message> shouldAcceptMessage = new Predicate<Message>() {
            public boolean test(Message message) {
                return true;
            }
        };

        // should collect payload message
        Collector collector = new Collector(config);
        collector.listenForResponses(topic, shouldAcceptMessage);
        ChannelManager channelManager = _g(collector, "channelManager");

        Message message = TestUtils.createMsbResponseMessage();

        channelManager.emit(Event.MESSAGE_EVENT, message);
        Collection<?> payloadMessages = _g(collector, "payloadMessages");
        assertTrue(payloadMessages.contains(message));

        // should collect acknowledge message and enable acknowledge timeout
        final Holder<Boolean> ackTimeoutCalled = new Holder<Boolean>();
        ackTimeoutCalled.value = false;

        config.setWaitForResponses(0);
        config.setAckTimeout(10000);
        collector = new Collector(config) {
            @Override
            protected void enableAckTimeout() {
                ackTimeoutCalled.value = true;
            }
        };
        collector.listenForResponses(topic, shouldAcceptMessage);
        channelManager = _g(collector, "channelManager");

        message = TestUtils.createMsbAckMessage();

        channelManager.emit(Event.MESSAGE_EVENT, message);
        Collection<?> acknowledgeMessages = _g(collector, "ackMessages");
        assertTrue(acknowledgeMessages.contains(message));
        assertTrue(ackTimeoutCalled.value);
    }
}
