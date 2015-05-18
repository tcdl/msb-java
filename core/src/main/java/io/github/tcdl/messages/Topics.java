package io.github.tcdl.messages;

/**
 * Created by rdro on 4/23/2015.
 */
public class Topics {

    private String to;
    private String response;

    public Topics withTo(String to) {
        this.to = to;
        return this;
    }

    public Topics withResponse(String response) {
        this.response = response;
        return this;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public String getTo() {
        return to;
    }

    public String getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "Topics [to=" + to + ", response=" + response + "]";
    }
}
