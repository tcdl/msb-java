package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.ExchangeType;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class AmqpAdapterFactoryTest {

    private static final Config CONFIG = ConfigFactory.load("reference.conf");

    final Charset charset = Charset.forName("UTF-8");
    final String host = "127.0.0.1";
    final int port = 5672;
    final String username = "user";
    final String password = "pwd";
    final String virtualHost = "127.10.10.10";
    final boolean useSSL = false;
    final String groupId = "msb-java";
    final boolean durable = false;
    final int heartbeatIntervalSec = 1;
    final long networkRecoveryIntervalMs = 5000;
    final int prefetchCount = 1;

    @Mock
    AmqpConnectionManager mockConnectionManager;

    @Mock
    ConnectionFactory mockConnectionFactory;

    @Mock
    Connection mockConnection;

    AmqpBrokerConfig amqpConfig;
    AmqpAdapterFactory amqpAdapterFactory;
    MsbConfig msbConfigurations;

    @Before
    public void setUp() {

        msbConfigurations = new MsbConfig(CONFIG);

        amqpConfig = new AmqpBrokerConfig(charset, host, port,
                Optional.of(username), Optional.of(password), Optional.of(virtualHost), useSSL, Optional.of(groupId), durable, ExchangeType.FANOUT,
                heartbeatIntervalSec, networkRecoveryIntervalMs, prefetchCount);

        amqpAdapterFactory = new AmqpAdapterFactory() {

            @Override
            public AmqpConsumerAdapter createConsumerAdapter(String topic, boolean isResponseTopic) {
                return new AmqpConsumerAdapter(topic, ExchangeType.FANOUT, Collections.emptySet(), amqpConfig, mockConnectionManager, isResponseTopic);
            }

            @Override
            protected ConnectionFactory createConnectionFactory() {
                return mockConnectionFactory;
            }

            @Override
            protected AmqpBrokerConfig createAmqpBrokerConfig(MsbConfig msbConfig) {
                return amqpConfig;
            }

            @Override
            protected AmqpConnectionManager createConnectionManager(Connection connection) {
                return mockConnectionManager;
            }

            @Override
            protected Connection createConnection(ConnectionFactory connectionFactory) {
                return mockConnection;
            }

        };
    }

    @Test
    public void testConnectionFactoryConfiguration() {
        amqpAdapterFactory.init(msbConfigurations);
        verify(mockConnectionFactory).setHost(eq(host));
        verify(mockConnectionFactory).setPort(eq(port));
        verify(mockConnectionFactory).setUsername(eq(username));
        verify(mockConnectionFactory).setPassword(eq(password));
        verify(mockConnectionFactory).setVirtualHost(eq(virtualHost));
    }

    @Test
    public void testInit() {
        amqpAdapterFactory.init(msbConfigurations);
        assertEquals(amqpAdapterFactory.getAmqpBrokerConfig(), amqpConfig);
        assertEquals(amqpAdapterFactory.getConnectionManager(), mockConnectionManager);
    }

    @Test
    public void testInitGroupIdWithServiceName() {
        AmqpBrokerConfig amqpBrokerConfig = new AmqpAdapterFactory().createAmqpBrokerConfig(msbConfigurations);
        assertEquals(amqpBrokerConfig.getGroupId().get(), msbConfigurations.getServiceDetails().getName());
    }

    @Test
    public void testShutdown() {
        amqpAdapterFactory.init(msbConfigurations);
        amqpAdapterFactory.shutdown();
        try {
            verify(mockConnectionManager).close();
        } catch (IOException e) {
            fail("Can't invoke ConnectionManager.close()");
        }
    }
}
