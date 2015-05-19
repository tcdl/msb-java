package io.github.tcdl.events;

/**
 * Created by rdro on 4/23/2015.
 */
public enum Event {

    PRODUCER_NEW_TOPIC_EVENT("newProducerOnTopic"),
    PRODUCER_NEW_MESSAGE_EVENT("newProducedMessage"),
    CONSUMER_NEW_TOPIC_EVENT("newConsumerOnTopic"),
    CONSUMER_REMOVED_TOPIC_EVENT("removedConsumerOnTopic"),
    CONSUMER_NEW_MESSAGE_EVENT("newConsumedMessage"),

    MESSAGE_EVENT("message"),
    ACKNOWLEDGE_EVENT("ack"),
    PAYLOAD_EVENT("payload"),
    RESPONSE_EVENT("response"),
    RESPONDER_EVENT("responder"),

    ERROR_EVENT("err"),
    END_EVENT("end");

    private String name;

    Event(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
