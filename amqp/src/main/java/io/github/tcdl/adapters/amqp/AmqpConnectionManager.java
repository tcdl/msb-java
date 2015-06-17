package io.github.tcdl.adapters.amqp;

import io.github.tcdl.exception.ChannelException;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;

/**
 * To work with AMQP broker (for example RabbitMQ) we use single connection.
 * The responsibility of this singleton class is to manage the lifecycle of that connection.
 */
public class AmqpConnectionManager {
    private static Logger LOG = LoggerFactory.getLogger(AmqpConnectionManager.class);

    private Connection connection;

    public AmqpConnectionManager(Connection connection) throws ChannelException {
        this.connection = connection;
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
