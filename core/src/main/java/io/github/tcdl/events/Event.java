package io.github.tcdl.events;

/**
 * @author rdro
 * @since 4/23/2015
 */
public enum Event {

    MESSAGE_EVENT("message"),
    ACKNOWLEDGE_EVENT("ack"),
    RESPONSE_EVENT("response"),
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
