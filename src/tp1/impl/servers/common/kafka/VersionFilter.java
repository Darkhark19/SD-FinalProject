package tp1.impl.servers.common.kafka;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import org.apache.cassandra.streaming.ReplicationFinishedVerbHandler;
import tp1.api.service.rest.RestDirectory;
import tp1.api.service.rest.RestRepDirectory;

import java.io.IOException;

public class VersionFilter implements ContainerResponseFilter {
    ReplicationManager repManager;

    public VersionFilter( ReplicationManager repManager){
        this.repManager = repManager;
    }
    @Override
    public void filter(ContainerRequestContext containerRequestContext, ContainerResponseContext containerResponseContext) throws IOException {
        containerResponseContext.getHeaders().add(RestRepDirectory.HEADER_VERSION,repManager.getCurrentVersion());
    }
}
