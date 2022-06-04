package tp1.impl.servers.rest;

import org.glassfish.jersey.server.ResourceConfig;
import tp1.api.service.java.Files;
import tp1.impl.servers.rest.util.CustomLoggingFilter;
import tp1.impl.servers.rest.util.GenericExceptionMapper;
import util.Debug;
import util.Token;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DropboxFilesServer extends AbstractRestServer {
    private static Logger Log = Logger.getLogger(DropboxFilesServer.class.getName());
    public static final int PORT = 5678;
    private final String apiKey;
    private final boolean flag;
    private final String apiSecret;
    private final String accessTokenStr;
    DropboxFilesServer(int port,boolean flag, String key, String secret, String token) {
        super(Log, Files.SERVICE_NAME, port);
        this.accessTokenStr = token;
        this.apiKey = key;
        this.apiSecret = secret;
        this.flag = flag;
    }
    @Override
    void registerResources(ResourceConfig config) {
        config.register(new DropboxFilesResource(flag,apiKey,apiSecret,accessTokenStr));
        config.register( GenericExceptionMapper.class );
        config.register( CustomLoggingFilter.class);
    }
    //arg0 - domain, arg1 - boolean , arg2 - serverSecret, arg3 - APIkey, arg4 - APISecret, arg5 -AccessToken,
    public static void main(String[] args) throws Exception {

        Debug.setLogLevel( Level.INFO, Debug.TP1);

        Token.set( args.length == 0 ? "" : args[1] );
        new DropboxFilesServer(PORT,Boolean.parseBoolean(args[0]),args[2],args[3],args[4]).start();
    }
}
