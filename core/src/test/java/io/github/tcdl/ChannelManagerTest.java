package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.xml.ws.Holder;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.SingleArgumentAdapter;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;

/**
 * Created by rdro on 4/24/2015.
 */
@Ignore("These tests fail from time to time")
/*
  ChannelManagerTest.test_receiveMessage:156 » NullPointer
  ChannelManagerTest.test_createOrFindConsumer:81 » NullPointer
  ChannelManagerTest.test_publishMessage:125 » IllegalState Task already schedul
 */
public class ChannelManagerTest {

	private MsbMessageOptions config;
	private ChannelManager channelManager;

	@Before
	public void setUp() {
		this.config = TestUtils.createSimpleConfig();
		channelManager = ChannelManager.getInstance();
	}

	@After
	public void cleanUp() {
		channelManager.removeProducer(config.getNamespace());
	}

	@Test
	public void test_createOrFindProducer() {
		final Holder<Boolean> newProducerTopicEventFired = new Holder<>();
		final Holder<String> topicName = new Holder<>();

		channelManager.on(ChannelManager.PRODUCER_NEW_TOPIC_EVENT, new SingleArgumentAdapter<String>() {
			@Override
			public void onEvent(String topic) {
				newProducerTopicEventFired.value = true;
				topicName.value = topic;
			}
		});

		String topic = config.getNamespace();
		channelManager.removeProducer(topic);
		Producer producer = channelManager.findOrCreateProducer(topic);

		assertNotNull(producer);
		assertTrue(newProducerTopicEventFired.value);
		assertEquals(topic, topicName.value);
	}

	@Test
	public void test_createOrFindConsumer() throws Exception {
		final Holder<Boolean> newConsumerTopicFired = new Holder<>();
		final Holder<String> topicName = new Holder<>();

		channelManager.on(ChannelManager.CONSUMER_NEW_TOPIC_EVENT, new SingleArgumentAdapter<String>() {
			@Override
			public void onEvent(String topic) {
				newConsumerTopicFired.value = true;
				topicName.value = topic;
			}
		});

		String topic = config.getNamespace();
		channelManager.removeConsumer(topic);
		Consumer consumer = channelManager.findOrCreateConsumer(topic, config);

		assertNotNull(consumer);
		assertTrue(newConsumerTopicFired.value);
		assertEquals(topic, topicName.value);
	}

	@Test
	public void test_removeConsumer() {
		final Holder<Boolean> consumerRemovedTopicEventFired = new Holder<>();
		final Holder<String> topicName = new Holder<>();

		channelManager.on(ChannelManager.CONSUMER_REMOVED_TOPIC_EVENT, new SingleArgumentAdapter<String>() {
			@Override
			public void onEvent(String topic) {
				consumerRemovedTopicEventFired.value = true;
				topicName.value = topic;
			}
		});

		String topic = config.getNamespace();
		channelManager.findOrCreateConsumer(topic, config);
		channelManager.removeConsumer(topic);

		assertTrue(consumerRemovedTopicEventFired.value);
		assertEquals(topic, topicName.value);
	}

	@Test
	public void test_publishMessage() {
		final Holder<Boolean> producerNewMessageEventFired = new Holder<>();
		final Holder<String> topicName = new Holder<>();

		channelManager.on(ChannelManager.PRODUCER_NEW_MESSAGE_EVENT, new SingleArgumentAdapter<String>() {
			@Override
			public void onEvent(String topic) {
				producerNewMessageEventFired.value = true;
				topicName.value = topic;
			}
		});

		String topic = config.getNamespace();
		Producer producer = channelManager.findOrCreateProducer(topic);
		Message message = TestUtils.createSimpleMsbMessage();
		producer.publish(message);

		assertTrue(producerNewMessageEventFired.value);
		assertEquals(topic, topicName.value);
	}

	@Test
	public void test_receiveMessage() {
		final Holder<Boolean> consumerNewMessageEventFired = new Holder<>();
		final Holder<String> topicName = new Holder<>();
		final Holder<Message> messageEvent = new Holder<>();

		channelManager.on(ChannelManager.CONSUMER_NEW_MESSAGE_EVENT, new SingleArgumentAdapter<String>() {
			@Override
			public void onEvent(String topic) {
				consumerNewMessageEventFired.value = true;
				topicName.value = topic;
			}
		});

		channelManager.on(ChannelManager.MESSAGE_EVENT, new SingleArgumentAdapter<Message>() {
			@Override
			public void onEvent(Message message) {
				messageEvent.value = message;
			}
		});

		String topic = config.getNamespace();

		Message message = TestUtils.createSimpleMsbMessage();
		channelManager.findOrCreateProducer(config.getNamespace()).publish(message);
		channelManager.findOrCreateConsumer(topic, config);

		assertTrue(consumerNewMessageEventFired.value);
		assertEquals(topic, topicName.value);
		assertNotNull(messageEvent.value);
	}
}
