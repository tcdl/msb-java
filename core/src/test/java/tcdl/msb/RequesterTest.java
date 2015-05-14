package tcdl.msb;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.messages.Message;
import tcdl.msb.messages.Topics;
import tcdl.msb.messages.payload.RequestPayload;
import tcdl.msb.support.TestUtils;

/**
 * Created by rdro on 4/27/2015.
 */
public class RequesterTest {

    private MsbMessageOptions config;
    private Requester requester;

    @Before
    public void setUp() {
        config = TestUtils.createSimpleConfig();
        Message message = TestUtils.createSimpleMsbMessage();
        message.withTopics(new Topics().withTo(config.getNamespace()));
        requester = new Requester(config, message);
    }

    // TODO write tests
    @Ignore("This test fails from time to time")
    /*
    java.lang.IllegalStateException: Task already scheduled or cancelled
	at java.util.Timer.sched(Timer.java:401)
	at java.util.Timer.schedule(Timer.java:193)
	at tcdl.msb.Collector.setTimeout(Collector.java:243)
	at tcdl.msb.Collector.enableTimeout(Collector.java:131)
	at tcdl.msb.Requester$2.onEvent(Requester.java:56)
	at tcdl.msb.Requester$2.onEvent(Requester.java:48)
	at tcdl.msb.events.TwoArgumentsAdapter.onEvent(TwoArgumentsAdapter.java:11)
	at tcdl.msb.Producer.publish(Producer.java:38)
	at tcdl.msb.Requester.publish(Requester.java:58)
	at RequesterTest.test_publish(RequesterTest.java:32)
     */
    @Test
    public void test_publish() throws Exception {
        requester.publish(new RequestPayload());
    }
}
