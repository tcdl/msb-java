package io.github.tcdl;

import io.github.tcdl.support.Utils;

/**
 * Created by rdro on 5/5/2015.
 */
public class ServiceDetails {

    private String name;
    private String version;
    private String instanceId;
    private String hostname;
    private String ip;
    private Integer pid;

    public ServiceDetails() {

    }

    public void init() {
        setName("service-name");
        setVersion("0.0.1");
        setInstanceId(Utils.generateId());

        // TODO
        setHostname("localhost");
        setIp("127.0.0.1");
        setPid(Utils.getPid());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }
}