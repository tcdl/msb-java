package tcdl.msb.adapters;

import com.rabbitmq.client.*;
import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.exception.ChannelException;

import java.io.IOException;

public class AmqpAdapter implements Adapter {
    private String topic;

    private Channel channel;
    private String exchangeName;

    public AmqpAdapter(String topic) {
        this.topic = topic;
        this.exchangeName = topic;

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
        MsbConfigurations configuration = MsbConfigurations.msbConfiguration();
        String groupId = configuration.getAmqpBrokerConf().getGroupId();
        boolean durable = configuration.getAmqpBrokerConf().isDurable();

        String queueName = generateQueueName(topic, groupId, durable);

        try {
            channel.queueDeclare(queueName, durable /* durable */, false /* exclusive */, true /*auto-delete */, null);
            channel.queueBind(queueName, exchangeName, "");

            channel.basicConsume(queueName, new DefaultConsumer(channel) {
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

    private String generateQueueName(String topic, String groupId, boolean durable) {
        return topic + "." + groupId + "." + (durable ? "d" : "t");
    }
}
