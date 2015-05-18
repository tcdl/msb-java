package io.github.tcdl.adapters;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.tcdl.config.MsbConfigurations;

import java.io.IOException;

public class AmqpConnectionManager {
    private static AmqpConnectionManager instance = new AmqpConnectionManager();

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
    }

    private static AmqpConnectionManager getInstance() {
        return instance;
    }

    public static Connection obtainConnection() {
        return getInstance().connection;
    }
}
