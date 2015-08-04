package io.github.tcdl.msb.message.payload;

public class Body {

    private String body;

    public Body() {
    }

    public Body(String body) {
        this.body = body;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Body body1 = (Body) o;

        return !(body != null ? !body.equals(body1.body) : body1.body != null);

    }

    @Override
    public int hashCode() {
        return body != null ? body.hashCode() : 0;
    }
}