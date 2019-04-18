package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.api.exception.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ActiveMQConnectionManager {

    private static Logger LOG = LoggerFactory.getLogger(ActiveMQConnectionManager.class);

    private ConnectionFactory connectionFactory;
    private Map<String, Connection> connectionsByClientId;
    private Map<String, Consumer<Connection>> connectionCloseListeners;

    public ActiveMQConnectionManager(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.connectionsByClientId = new ConcurrentHashMap<>();
        this.connectionCloseListeners = new ConcurrentHashMap<>();
    }

    /**
     * @throws ChannelException if some problems during connecting to Broker were occurred
     */
    public Connection obtainConnection(String clientId) {
        try {
            if (!connectionsByClientId.containsKey(clientId)) {
                Connection connection = connectionFactory.createConnection();
                if (connection.getClientID() == null) {
                    connection.setClientID(clientId != null ? clientId : UUID.randomUUID().toString());
                }
                connection.start();
                connectionsByClientId.put(clientId, connection);
            }

            LOG.info("ActiveMQ connection obtained.");
            return connectionsByClientId.get(clientId);
        } catch (JMSException e) {
            throw new ChannelException("Could not obtain ActiveMQ connection", e);
        }
    }

    public void addConnectionCloseListener(String clientId, Consumer<Connection> connectionCloseListener) {
        connectionCloseListeners.put(clientId, connectionCloseListener);
    }

    public void close() {
        connectionsByClientId.forEach((clientId, connection) -> {
            try {
                if (connectionCloseListeners.containsKey(clientId)) {
                    connectionCloseListeners.get(clientId).accept(connection);
                }
                connection.stop();
                connection.close();
            } catch (JMSException e) {
                LOG.error("Error closing connection with exception", e);
            }
        });
    }
}
