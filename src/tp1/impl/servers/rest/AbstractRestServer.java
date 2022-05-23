package tp1.impl.servers.rest;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import tp1.impl.discovery.Discovery;
import tp1.tls.InsecureHostnameVerifier;
import util.IP;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

public abstract class AbstractRestServer {

    protected static String SERVER_BASE_URI = "https://%s:%s/rest";

    final int port;
    final String service;
    final private Logger Log;

    protected AbstractRestServer(Logger log, String service, int port) {
        this.service = service;
        this.port = port;
        this.Log = log;
    }


    protected void start() {
        try {
            String ip = IP.hostAddress();
            String serverURI = String.format(SERVER_BASE_URI, ip, port);

            ResourceConfig config = new ResourceConfig();

            registerResources(config);

            System.err.println(">>>>>" + port);
            HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
            JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, SSLContext.getDefault());

            Log.info(String.format("%s Server ready @ %s\n", service, serverURI));

            Discovery.getInstance().announce(service, serverURI);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }

    abstract void registerResources(ResourceConfig config);

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
    }
}
