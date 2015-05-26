package io.github.tcdl;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequesterResponderIT {

    public static final Logger LOG = LoggerFactory.getLogger(RequesterResponderIT.class);
  
    private MsbConfigurations msbConf;

    @Before
    public void setUp() throws Exception {       
        this.msbConf = MsbConfigurations.msbConfiguration();
    }

    @Test
    public void testResponderServerRecievedMessage() throws Exception {        
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-testrecieved");
        CountDownLatch requestRecieved = new CountDownLatch(1);
        
        //Create and send request message
        Requester requester = new Requester(messageOptions, null);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        requester.publish(requestPayload);
        
        ResponderServer.create(messageOptions)
        .use(((request, response) -> {
            requestRecieved.countDown();
        }))
        .listen();

        assertTrue("Message was not recieved", requestRecieved.await(500, TimeUnit.MILLISECONDS));
    }
    
    @Test
    @Ignore("need fix of the code")
    public void testRequestGetResponseMessage() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-testgetack");
        messageOptions.setAckTimeout(10000);
        messageOptions.setWaitForResponses(1);
        CountDownLatch ackSend = new CountDownLatch(1);        
        
        //Create and send request message, wait for ack       
        Requester requester = new Requester(messageOptions, null);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        requester.publish(requestPayload);

        //listen for message and send response
        List<Message> sendedAcks = new LinkedList<Message>();
        ResponderServer.create(messageOptions)
                .use(((request, response) -> {
                    System.out.println("YES i DOOOO");
                    response.sendAck(100, 2, null);
                    sendedAcks.add(response.getResponseMessage());
                    ackSend.countDown();
                }))
                .listen();
      
        assertTrue("Message was not ack", ackSend.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(4000);
        assertFalse(requester.isMessageAcknowledged());
    }

}
