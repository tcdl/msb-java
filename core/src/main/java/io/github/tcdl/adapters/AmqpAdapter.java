package io.github.tcdl.adapters;

import com.rabbitmq.client.*;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.ChannelException;

import java.io.IOException;

import org.apache.commons.lang3.Validate;

public class AmqpAdapter implements Adapter {
    private String topic;

    private Channel channel;
    private String exchangeName;
    private String consumerTag;
    private MsbConfigurations configuration;

    public AmqpAdapter(String topic, MsbConfigurations msbConfig) {
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(msbConfig, "the 'msbConfig' must not be null");
        this.topic = topic;
        this.exchangeName = topic;
        this.configuration = msbConfig;

        Connection connection = AmqpConnectionManager.obtainConnection();
        try {
            channel = connection.createChannel();
            channel.exchangeDeclare(exchangeName, "fanout", false /* durable */, true /* auto-delete */, null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to setup channel from ActiveMQ connection", e);
        }
    }

    @Override
    public void publish(String jsonMessage) throws ChannelException {
        try {
            channel.basicPublish(exchangeName, "" /* routing key */, MessageProperties.PERSISTENT_BASIC, jsonMessage.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to publish message '%s' into exchange '%s'", jsonMessage, exchangeName), e);
        }
    }

    @Override
    public void subscribe(RawMessageHandler msgHandler) {       
        String groupId = configuration.getAmqpBrokerConf().getGroupId();
        boolean durable = configuration.getAmqpBrokerConf().isDurable();

        String queueName = generateQueueName(topic, groupId, durable);

        try {
            channel.queueDeclare(queueName, durable /* durable */, false /* exclusive */, true /*auto-delete */, null);
            channel.queueBind(queueName, exchangeName, "");

            consumerTag = channel.basicConsume(queueName, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String bodyStr = new String(body);
                    msgHandler.onMessage(bodyStr);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to subscribe to topic %s", topic), e);
        }
    }

    @Override
    public void unsubscribe() {
        try {
            channel.basicCancel(consumerTag);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to unsubscribe from topic %s", topic), e);
        }
    }

    private String generateQueueName(String topic, String groupId, boolean durable) {
        return topic + "." + groupId + "." + (durable ? "d" : "t");
    }
}
