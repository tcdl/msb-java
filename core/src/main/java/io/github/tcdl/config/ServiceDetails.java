package io.github.tcdl.config;

import static io.github.tcdl.config.ConfigurationUtil.getString;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jcabi.manifests.Manifests;

import io.github.tcdl.support.Utils;
import com.typesafe.config.Config;

/**
 * Class contains configuration data related to service instance *
 */
public final class ServiceDetails {
    public final static Logger LOG = LoggerFactory.getLogger(ServiceDetails.class);

    private final String name;
    private final String version;
    private final String instanceId;
    private final String hostname;
    private final String ip;
    private final long pid;

    @JsonCreator
    private ServiceDetails(@JsonProperty("name") String name, @JsonProperty("version") String version, @JsonProperty("instanceId") String instanceId,
            @JsonProperty("hostname") String hostname, @JsonProperty("ip") String ip, @JsonProperty("pid") long pid) {

        this.name = name;
        this.version = version;
        this.instanceId = instanceId;
        this.hostname = hostname;
        this.ip = ip;
        this.pid = pid;
    }

    public static class ServiceDetailsBuilder {

        private String name;
        private String version;
        private String instanceId;
        private String hostname;
        private String ip;
        private long pid;

        public ServiceDetailsBuilder(Config config) {

            name = Manifests.read("Specification-Title");
            version = Manifests.read("Specification-Version");
            instanceId = Utils.generateId();

            name = getString(config, "name", name);
            version = getString(config, "version", version);
            instanceId = getString(config, "instanceId", instanceId);

            hostname = getHostInfo().getHostName();
            ip = getHostInfo().getHostAddress();
            pid = getPID();
        }

        public ServiceDetailsBuilder() {
        }

        public ServiceDetails build() {
            return new ServiceDetails(name, version, instanceId, hostname, ip, pid);
        }

        private static InetAddress getHostInfo() {
            InetAddress hostInfo = null;
            try {
                hostInfo = InetAddress.getLocalHost();
            } catch (UnknownHostException ex) {
                LOG.error("Fail to retrieve host info", ex);
            }
            return hostInfo;
        }

        private static long getPID() {
            String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            return Long.parseLong(processName.split("@")[0]);
        }
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getHostname() {
        return hostname;
    }

    public String getIp() {
        return ip;
    }

    public long getPid() {
        return pid;
    }

    @Override
    public String toString() {
        return "ServiceDetails [name=" + name + ", version=" + version + ", instanceId=" + instanceId + ", hostname="
                + hostname + ", ip=" + ip + ", pid=" + pid + "]";
    }

}