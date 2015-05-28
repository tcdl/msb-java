package io.github.tcdl.events;

/**
 * @author rdro
 * @since 4/23/2015
 */
public enum Event {

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
