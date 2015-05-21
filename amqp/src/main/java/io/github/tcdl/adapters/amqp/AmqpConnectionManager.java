package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.tcdl.config.MsbConfigurations;

import java.io.IOException;

/**
 * To work with AMQP broker (for example RabbitMQ) we use single connection.
 * The responsibility of this singleton class is to manage the lifecycle of that connection.
 */
public class AmqpConnectionManager {
    /**
     * Usage of "Initialization-on-demand holder" idiom to have thread-safe lazy loading of this singleton.
     * We cannot fall back to eager initialization because otherwise
     */
    private static class LazyHolder {
        private static final AmqpConnectionManager INSTANCE = new AmqpConnectionManager();
    }

    private Connection connection;

    private AmqpConnectionManager() {
        MsbConfigurations configuration = MsbConfigurations.msbConfiguration();

        String host = configuration.getAmqpBrokerConf().getHost();
        int port = configuration.getAmqpBrokerConf().getPort();

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);

        try {
            connection = connectionFactory.newConnection();
        } catch (IOException e) {
            throw new RuntimeException("Failed to obtain connection to AMQP broker", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static AmqpConnectionManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    public static Connection obtainConnection() {
        return getInstance().connection;
    }
}
