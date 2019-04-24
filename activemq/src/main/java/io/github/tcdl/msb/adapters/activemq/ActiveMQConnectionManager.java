package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.api.exception.ChannelException;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ActiveMQConnectionManager {

    private static Logger LOG = LoggerFactory.getLogger(ActiveMQConnectionManager.class);

    private PooledConnectionFactory connectionFactory;
    private Map<String, Connection> connectionsByClientId;
    private Map<String, Consumer<Connection>> connectionCloseListeners;

    public ActiveMQConnectionManager(PooledConnectionFactory connectionFactory) {
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
                Connection connection = openConnection();
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

        connectionFactory.clear();
        connectionFactory.stop();
    }

    private Connection openConnection() throws JMSException {
        Connection connection = connectionFactory.createConnection();
        if (connection == null) {
            connectionFactory.initConnectionsPool();
            connectionFactory.start();
            connection = connectionFactory.createConnection();
        }
        return connection;
    }
}
