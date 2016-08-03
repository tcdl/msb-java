package io.github.tcdl.msb.jmeter.sampler;

import java.util.Objects;

import static io.github.tcdl.msb.support.Utils.ifNull;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

/**
 * Created by rdro-tc on 28.07.16.
 */
public class RequesterConfig {

    public final static String TEST_ELEMENT_CONFIG = "TestElement.msb_requester";

    private String host;
    private Integer port;
    private String virtualHost;
    private String userName;
    private String password;
    private String namespace;
    private String forwardNamespace;
    private Boolean waitForResponses;
    private Integer numberOfResponses;
    private Integer timeout;
    private String requestPayload;

    public RequesterConfig() {
        setDefaults();
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getForwardNamespace() {
        return forwardNamespace;
    }

    public void setForwardNamespace(String forwardNamespace) {
        this.forwardNamespace = forwardNamespace;
    }

    public Boolean getWaitForResponses() {
        return waitForResponses;
    }

    public void setWaitForResponses(Boolean waitForResponses) {
        this.waitForResponses = waitForResponses;
    }

    public Integer getNumberOfResponses() {
        return numberOfResponses;
    }

    public void setNumberOfResponses(Integer numberOfResponses) {
        this.numberOfResponses = numberOfResponses;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public String getRequestPayload() {
        return requestPayload;
    }

    public void setRequestPayload(String requestPayload) {
        this.requestPayload = requestPayload;
    }

    public void setDefaults() {
        this.host = defaultIfBlank(this.host, "localhost");
        this.port = this.port == null || this.port <= 0 ? 5672 : this.port;
        this.virtualHost = defaultIfBlank(this.virtualHost, "/");
        this.userName = defaultIfBlank(this.userName, "guest");
        this.password = defaultIfBlank(this.password, "guest");
        this.namespace = defaultIfBlank(this.namespace, "jmeter:test");
        this.waitForResponses = ifNull(this.waitForResponses, true);
        this.numberOfResponses = ifNull(this.numberOfResponses, 1);
        this.timeout =  this.timeout == null || this.timeout <= 0 ? 3000 : this.timeout;
        this.requestPayload = defaultIfBlank(this.requestPayload, "{}");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequesterConfig that = (RequesterConfig) o;
        return Objects.equals(host, that.host) &&
                Objects.equals(port, that.port) &&
                Objects.equals(virtualHost, that.virtualHost) &&
                Objects.equals(userName, that.userName) &&
                Objects.equals(password, that.password) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(forwardNamespace, that.forwardNamespace) &&
                Objects.equals(waitForResponses, that.waitForResponses) &&
                Objects.equals(numberOfResponses, that.numberOfResponses) &&
                Objects.equals(timeout, that.timeout) &&
                Objects.equals(requestPayload, that.requestPayload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, virtualHost, userName, password, namespace, forwardNamespace, waitForResponses, numberOfResponses, timeout, requestPayload);
    }
}
