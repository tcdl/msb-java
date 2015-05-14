package tcdl.msb.messages.payload;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 4/23/2015.
 */
@SuppressWarnings("rawtypes")
public class BasicPayload <T extends BasicPayload> {

    private Map<String, String> headers = new HashMap<>();
    private Map<?, ?> body = new HashMap<>();

    @SuppressWarnings("unchecked")
    public T withHeaders(Map<String, String> headers) {
        this.headers = headers;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withBody(Map<?, ?> body) {
        this.body = body;
        return (T) this;
    }

    public Map<?, ?> getHeaders() {
        return headers;
    }

    public Map<?, ?> getBody() {
        return body;
    }

    @Override
    public String toString() {
        return "BasicPayload [headers=" + headers + ", body=" + body + "]";
    }
}
