package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.api.exception.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.util.UUID;

public class ActiveMQConnectionManager {

    private static Logger LOG = LoggerFactory.getLogger(ActiveMQConnectionManager.class);

    private ConnectionFactory connectionFactory;

    public ActiveMQConnectionManager(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * @throws ChannelException if some problems during connecting to Broker were occurred
     */
    public Connection obtainConnection() {
        try {
            Connection connection = connectionFactory.createConnection();
            if (connection.getClientID() == null) {
                connection.setClientID(UUID.randomUUID().toString());
            }
            connection.start();
            LOG.info("ActiveMQ connection obtained.");
            return connection;
        } catch (JMSException e) {
            throw new ChannelException("Could not obtain ActiveMQ connection", e);
        }
    }

    public void close() throws JMSException {
        // All connections are closed after idle timeout
    }

    /**
     * @throws ChannelException if some problems during connecting to Broker were occurred
     */
    public Connection createConnection() {
        try {
            LOG.info("Opening ActiveMQ connection");

            Connection connection = connectionFactory.createConnection();
            connection.setClientID(UUID.randomUUID().toString());
            connection.start();

            LOG.info("ActiveMQ connection opened.");
            return connection;
        } catch (JMSException e) {
            throw new ChannelException("Failed to obtain connection to ActiveMQ broker", e);
        }
    }
}
