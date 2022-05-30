package tp1.impl.servers.rest;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tp1.api.service.java.Users;
import tp1.impl.servers.rest.util.CustomLoggingFilter;
import tp1.impl.servers.rest.util.GenericExceptionMapper;
import util.Debug;
import util.Token;


public class UsersRestServer extends AbstractRestServer {
	public static final int PORT = 3456;
	
	private static Logger Log = Logger.getLogger(UsersRestServer.class.getName());

	UsersRestServer( int port ) {
		super( Log, Users.SERVICE_NAME, port);
	}
	
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( UsersResources.class ); 
		config.register( GenericExceptionMapper.class);
		config.register( CustomLoggingFilter.class);
	}
	
	
	public static void main(String[] args) throws Exception {

		Debug.setLogLevel( Level.INFO, Debug.TP1);
		
		Token.set( args.length == 0 ? "" : args[0] );
		System.out.println(args[0]);
		new UsersRestServer(PORT).start();

	}	
}