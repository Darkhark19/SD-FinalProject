package tp1.impl.servers.rest;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tp1.api.service.java.Files;
import tp1.impl.servers.rest.util.CustomLoggingFilter;
import tp1.impl.servers.rest.util.GenericExceptionMapper;
import util.Debug;
import util.Token;

public class FilesRestServer extends AbstractRestServer {
	public static final int PORT = 5678;
	
	private static Logger Log = Logger.getLogger(FilesRestServer.class.getName());


	FilesRestServer( int port ) {
		super(Log, Files.SERVICE_NAME, port);
	}
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( FilesResources.class ); 
		config.register( GenericExceptionMapper.class );
		config.register( CustomLoggingFilter.class);
	}
	
	public static void main(String[] args) throws Exception {

		Debug.setLogLevel( Level.INFO, Debug.TP1);
		
		Token.set( args.length == 0 ? "" : args[0] );
		System.out.println(args[0]);
		new FilesRestServer(PORT).start();
	}	
}