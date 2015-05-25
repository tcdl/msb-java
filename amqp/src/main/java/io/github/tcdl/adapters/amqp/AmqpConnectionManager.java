package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.tcdl.config.MsbConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * To work with AMQP broker (for example RabbitMQ) we use single connection.
 * The responsibility of this singleton class is to manage the lifecycle of that connection.
 */
public class AmqpConnectionManager {
    private static Logger log = LoggerFactory.getLogger(AmqpConnectionManager.class);

    private Connection connection;

    public AmqpConnectionManager(AmqpBrokerConfig adapterConfig) {
        String host = adapterConfig.getHost();
        int port = adapterConfig.getPort();

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);

        try {
            log.info(String.format("Opening AMQP connection to host = %s, port = %s...", host, port));
            connection = connectionFactory.newConnection();
            log.info("AMQP connection opened.");
        } catch (IOException e) {
            throw new RuntimeException("Failed to obtain connection to AMQP broker", e);
        }

        //TODO: close on client demand
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

    public Connection obtainConnection() {
        return connection;
    }
}
