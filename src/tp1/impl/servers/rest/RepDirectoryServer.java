package tp1.impl.servers.rest;

import org.glassfish.jersey.server.ResourceConfig;
import tp1.api.service.java.Directory;
import tp1.impl.servers.common.kafka.RepDirectory;
import tp1.impl.servers.common.kafka.ReplicationManager;
import tp1.impl.servers.common.kafka.VersionFilter;
import tp1.impl.servers.rest.util.CustomLoggingFilter;
import tp1.impl.servers.rest.util.GenericExceptionMapper;
import util.Debug;
import util.Token;

import java.util.logging.Level;
import java.util.logging.Logger;

public class RepDirectoryServer extends AbstractRestServer{
    public static final int PORT = 8080;
    private static Logger Log = Logger.getLogger(RepDirectoryServer.class.getName());

    RepDirectoryServer( int port) {
        super(Log, Directory.SERVICE_NAME, port);
    }

    @Override
    void registerResources(ResourceConfig config) {
        ReplicationManager repManager = new ReplicationManager();
        config.register( new RepDirectoryResource(repManager) );
        config.register(new VersionFilter(repManager));
        config.register( GenericExceptionMapper.class );
        config.register( CustomLoggingFilter.class);
    }

    public static void main(String[] args) throws Exception {

        Debug.setLogLevel( Level.INFO, Debug.TP1);

        Token.set( args.length == 0 ? "" : args[0] );

        new RepDirectoryServer(PORT).start();
    }
}
