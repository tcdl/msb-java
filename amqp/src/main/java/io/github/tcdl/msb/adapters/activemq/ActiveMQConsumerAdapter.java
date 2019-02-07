package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.adapters.ConsumerAdapter;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Optional;

public class ActiveMQConsumerAdapter implements ConsumerAdapter {


    private final MessageConsumer consumer;

    public ActiveMQConsumerAdapter(String topic, Session session, boolean isResponseTopic) throws JMSException {
        Queue queue = session.createQueue("Consumer.MSB.VirtualTopic." + topic);

        consumer = session.createConsumer(queue);
    }

    @Override
    public void subscribe(RawMessageHandler onMessageHandler) {

    }

    @Override
    public void unsubscribe() {
        try {
            consumer.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<Long> messageCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Boolean> isConnected() {
        return Optional.empty();
    }
}
