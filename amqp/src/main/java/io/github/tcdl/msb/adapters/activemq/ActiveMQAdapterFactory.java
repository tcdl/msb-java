package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.AmqpRequestOptions;
import io.github.tcdl.msb.api.ExchangeType;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.api.exception.AdapterCreationException;
import io.github.tcdl.msb.config.MsbConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

public class ActiveMQAdapterFactory implements AdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQAdapterFactory.class);

    private MsbConfig msbConfig;
    private volatile Connection connection;
    private Session session;
    //private volatile ActiveMQConnectionFactory

    @Override
    public void init(MsbConfig msbConfig) {
        this.msbConfig = msbConfig;

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

        //todo: read configs from msbConfig
        connectionFactory.setBrokerURL("tcp://localhost:61616");
        connectionFactory.setUserName("admin");
        connectionFactory.setPassword("admin");

        //PooledConnectionFactory? -- recommended to use for producers

        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ProducerAdapter createProducerAdapter(String topic, RequestOptions requestOptions) {

        try {
            return new ActiveMQProducerAdapter(topic, requestOptions, session);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String topic, boolean isResponseTopic) {
        return null;
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String topic, ResponderOptions responderOptions, boolean isResponseTopic) {

        try {
            return new ActiveMQConsumerAdapter(topic, session, isResponseTopic);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean isUseMsbThreadingModel() {
        return false;
    }

    @Override
    public void shutdown() {

    }
}
