package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.github.tcdl.config.amqp.AmqpBrokerConfig;
import io.github.tcdl.exception.ChannelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * To work with AMQP broker (for example RabbitMQ) we use single connection.
 * The responsibility of this singleton class is to manage the lifecycle of that connection.
 */
public class AmqpConnectionManager {
    private static Logger LOG = LoggerFactory.getLogger(AmqpConnectionManager.class);

    private Connection connection;

    public AmqpConnectionManager(AmqpBrokerConfig adapterConfig) {
        ConnectionFactory connectionFactory = createConnectionFactory(adapterConfig);
        try {
            LOG.info(String.format("Opening AMQP connection to host = %s, port = %s, username = %s, password = %s, virtualHost = %s...", 
                    connectionFactory.getHost(), connectionFactory.getPort(), connectionFactory.getUsername(),
                    connectionFactory.getPassword(), connectionFactory.getVirtualHost()));
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
    
    protected ConnectionFactory createConnectionFactory(AmqpBrokerConfig adapterConfig) {
        String host = adapterConfig.getHost();
        int port = adapterConfig.getPort();
        Optional<String> username = adapterConfig.getUsername();
        Optional<String> password = adapterConfig.getPassword();
        Optional<String> virtualHost = adapterConfig.getVirtualHost();

        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        if(username.isPresent()) {
            connectionFactory.setUsername(username.get());
        }
        if(password.isPresent()) {
            connectionFactory.setPassword(password.get());
        }
        if(virtualHost.isPresent()) {
            connectionFactory.setVirtualHost(virtualHost.get());
        }
        return connectionFactory;
    }
    
    protected ConnectionFactory createConnectionFactory() {
        return new ConnectionFactory();
    }
}
