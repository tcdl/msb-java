package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.github.tcdl.config.amqp.AmqpBrokerConfig;

import io.github.tcdl.exception.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * To work with AMQP broker (for example RabbitMQ) we use single connection.
 * The responsibility of this singleton class is to manage the lifecycle of that connection.
 */
public class AmqpConnectionManager {
    private static Logger LOG = LoggerFactory.getLogger(AmqpConnectionManager.class);

    private Connection connection;

    public AmqpConnectionManager(AmqpBrokerConfig adapterConfig) {
        String host = adapterConfig.getHost();
        int port = adapterConfig.getPort();

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);

        try {
            LOG.info(String.format("Opening AMQP connection to host = %s, port = %s...", host, port));
            connection = connectionFactory.newConnection();
            LOG.info("AMQP connection opened.");
        } catch (IOException e) {
            throw new ChannelException("Failed to obtain connection to AMQP broker", e);
        }
    }

    public Connection obtainConnection() {
        return connection;
    }

    public void close() throws IOException {
        if (connection.isOpen()) {
            LOG.info("Closing AMQP connection...");
            connection.close();
            LOG.info("AMQP connection closed.");
        }
    }
}
