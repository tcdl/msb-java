package tcdl.msb.messages.payload;

public class ResponsePayload extends BasicPayload<ResponsePayload> {

    private Integer statusCode;
    private String bodyBase64;

    public ResponsePayload withStatusCode(int statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    public ResponsePayload withBodyBase64(String bodyBase64) {
        this.bodyBase64 = bodyBase64;
        return this;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getBodyBase64() {
        return bodyBase64;
    }

    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public void setBodyBase64(String bodyBase64) {
        this.bodyBase64 = bodyBase64;
    }

    @Override
    public String toString() {
        return "ResponsePayload [statusCode=" + statusCode + ", bodyBase64=" + bodyBase64 + ", getHeaders()="
                + getHeaders() + ", getBody()=" + getBody() + "]";
    }
}
