package io.github.tcdl.adapters.amqp;

import io.github.tcdl.config.amqp.AmqpBrokerConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * AmqpConnectionManager class managed connections to AMQP Broker.
 * Represented as a Singleton.
 *  
 * @author ysavchuk
 *
 */
public class AmqpConnectionManager {
    private static class LazyHolder {
        private static final AmqpConnectionManager INSTANCE = new AmqpConnectionManager();
    }

    //Connections Map
    private Map<AmqpBrokerConfig, Connection> connections = new HashMap<AmqpBrokerConfig, Connection>();

    /**
     * The constructor
     */
    private AmqpConnectionManager() {
       
    }

    public static AmqpConnectionManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    /**
     * Create new Connection to AMQP Broker and put it to Map 
     * with config as a key.
     * @param config - AmqpBrokerConfig
     * @return - Connection
     */
    public Connection obtainConnection(AmqpBrokerConfig config) {
        Connection connection = connections.get(config);
        if (connection == null) {
            String host = config.getHost();
            int port = config.getPort();

            ConnectionFactory connectionFactory = getConnectionFactory();
            connectionFactory.setHost(host);
            connectionFactory.setPort(port);

            try {
                connection = connectionFactory.newConnection();
                connections.put(config, connection);

            } catch (IOException e) {
                throw new RuntimeException(
                        String.format("Failed to obtain connection to AMQP broker. host:%s, port:%d", host, port), e);
            }
        }
        return connection;
    }
    
    private ConnectionFactory getConnectionFactory() {
        return new ConnectionFactory();

    }
    
}
