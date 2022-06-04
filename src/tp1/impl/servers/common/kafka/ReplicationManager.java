package tp1.impl.servers.common.kafka;

public class ReplicationManager {
    private Long version;

    public ReplicationManager() {
        version = -1L;
    }

    public void setVersion(Long version) {

        this.version = version;

    }

    public Long getCurrentVersion() {
        return version;
    }

}
