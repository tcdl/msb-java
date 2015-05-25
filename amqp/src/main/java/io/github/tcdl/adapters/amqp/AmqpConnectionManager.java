package io.github.tcdl.adapters.amqp;

import io.github.tcdl.config.amqp.AmqpBrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static Logger log = LoggerFactory.getLogger(AmqpConnectionManager.class);

    /**
     * Usage of "Initialization-on-demand holder" idiom to have thread-safe lazy loading of this singleton.
     * We cannot fall back to eager initialization because otherwise
     */
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
                log.info(String.format("Opening AMQP connection to host = %s, port = %s...", host, port));
                connection = connectionFactory.newConnection();
                connections.put(config, connection);
                log.info("AMQP connection opened.");

            } catch (IOException e) {
                throw new RuntimeException(
                        String.format("Failed to obtain connection to AMQP broker. host:%s, port:%d", host, port), e);
            }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    log.info("Closing AMQP connection...");
                    connection.close();
                    log.info("AMQP connection closed.");
                } catch (IOException e) {
                    log.error("Error while closing AMQP connection", e);
                }
            }
        });
        }
        return connection;
    }
    
    private ConnectionFactory getConnectionFactory() {
        return new ConnectionFactory();

    }
    
}
