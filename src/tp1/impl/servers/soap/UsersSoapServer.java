package tp1.impl.servers.soap;


import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import jakarta.xml.ws.Endpoint;
import tp1.impl.discovery.Discovery;
import util.IP;
import util.Token;

import javax.net.ssl.SSLContext;


public class UsersSoapServer {

	public static final int PORT = 13456;
	public static final String SERVICE_NAME = "users";
	public static String SERVER_BASE_URI = "https://%s:%s/soap";

	private static Logger Log = Logger.getLogger(UsersSoapServer.class.getName());

	public static void main(String[] args) throws Exception {
		Token.set( args.length > 0 ? args[0] : "" );

//		System.setProperty("com.sun.xml.ws.transport.http.client.HttpTransportPipe.dump", "true");
//		System.setProperty("com.sun.xml.internal.ws.transport.http.client.HttpTransportPipe.dump", "true");
//		System.setProperty("com.sun.xml.ws.transport.http.HttpAdapter.dump", "true");
//		System.setProperty("com.sun.xml.internal.ws.transport.http.HttpAdapter.dump", "true");

		Log.setLevel(Level.FINER);

		String ip = IP.hostAddress();
		String serverURI = String.format(SERVER_BASE_URI, ip, PORT);

		Discovery.getInstance().announce(SERVICE_NAME, serverURI);

		var configurator = new HttpsConfigurator(SSLContext.getDefault());

		var server = HttpsServer.create(new InetSocketAddress(ip, PORT), 0);
		server.setExecutor(Executors.newCachedThreadPool());
		server.setHttpsConfigurator(configurator);


		var endpoint = Endpoint.create(new SoapUsersWebService());
		endpoint.publish(server.createContext("/soap"));
		server.start();

		Log.info(String.format("%s Soap Server ready @ %s\n", SERVICE_NAME, serverURI));
	}
	
	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
	}

}
