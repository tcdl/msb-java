package tcdl.msb;

import tcdl.msb.support.Utils;

import static tcdl.msb.support.Utils.ifNull;

/**
 * Created by rdro on 5/5/2015.
 */
public class ServiceDetails {

    private String name;
    private String version;
    private String instanceId;
    private String hostName;
    private String ip;
    private Integer pid;

    public ServiceDetails() {

    }

    public void init() {
        setName("service-name");
        setVersion("0.0.1");
        setInstanceId(Utils.generateId());

        // TODO
        setHostName("localhost");
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

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
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