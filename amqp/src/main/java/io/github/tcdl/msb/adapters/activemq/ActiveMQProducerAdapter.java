package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.RequestOptions;

import javax.jms.*;

public class ActiveMQProducerAdapter implements ProducerAdapter {

    private final MessageProducer producer;
    private final Session session;

    public ActiveMQProducerAdapter(String topic, RequestOptions requestOptions, Session session) throws JMSException {
        this.session = session;

        Destination producerDestination = session.createTopic("VirtualTopic." + topic);
        // Create a producer from the session to the queue.
        producer = session.createProducer(producerDestination);
        producer.setDeliveryMode(1); //
    }

    @Override
    public void publish(String jsonMessage) {
        //simple publish
        try {
            // Create a message.
            TextMessage producerMessage = session.createTextMessage(jsonMessage);
            producer.send(producerMessage);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void publish(String jsonMessage, String routingKey) {
        publish(jsonMessage);
    }
}
