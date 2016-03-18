package io.github.tcdl.msb.config;

import static io.github.tcdl.msb.config.ConfigurationUtil.getOptionalString;
import static io.github.tcdl.msb.config.ConfigurationUtil.getString;
import io.github.tcdl.msb.support.Utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.typesafe.config.Config;

/**
 * Class contains configuration data related to service instance.
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

    public static class Builder {

        private String name;
        private String version;
        private String instanceId;
        private String hostname;
        private String ip;
        private long pid;

        public Builder(Config config) {
            name = getString(config, "name");
            version = getString(config, "version");
            
            Optional<String> optionalInstanceId = getOptionalString(config, "instanceId");
            instanceId = optionalInstanceId.isPresent() ? optionalInstanceId.get() : Utils.generateId(); 

            Optional<InetAddress> optionalHostInfo = getHostInfo();
            hostname = optionalHostInfo.isPresent() ? optionalHostInfo.get().getHostName() : "unknown";
            ip = optionalHostInfo.isPresent() ? optionalHostInfo.get().getHostAddress() : null;
            
            pid = getPID();
        }

        public Builder() {
        }

        public ServiceDetails build() {
            return new ServiceDetails(name, version, instanceId, hostname, ip, pid);
        }

        protected Optional<InetAddress> getHostInfo() {
            Optional<InetAddress> optionalHostInfo;
            try {
                optionalHostInfo = Optional.of(InetAddress.getLocalHost());
            } catch (UnknownHostException ex) {
                LOG.warn("Fail to retrieve host info", ex);
                optionalHostInfo = Optional.empty();
            }
            return optionalHostInfo;
        }

        protected long getPID() {
            String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            long pid;
            try {
                pid = Long.parseLong(processName.split("@")[0]);
            } catch (NumberFormatException ex) {
                LOG.warn("Fail to get Process ID", ex);
                pid = 0;
            }
            return pid;
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
                + hostname + String.valueOf(ip != null ? ", ip=" + ip : "") + ", pid=" + pid + "]";
    }

}