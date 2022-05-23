package tp1.impl.discovery;


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import util.Sleep;

/**
 * Performs service discovery. Used by servers to announce themselves, and clients
 * to discover services on demand.
 * 
 * @author smduarte
 *
 */
public class Discovery {
	private static Logger Log = Logger.getLogger(Discovery.class.getName());
	private static final String DELIMITER = "\t";

	static final int DISCOVERY_PERIOD = 1000;
	static final int DISCOVERY_TIMEOUT = 10000;
	static final InetSocketAddress DISCOVERY_ADDR = new InetSocketAddress("226.226.226.226", 2262);

	final Map<String, Set<URI>> discoveries = new ConcurrentHashMap<>();
	
	static Discovery instance;
	
	synchronized public static Discovery getInstance() {
		if( instance == null ) {
			instance = new Discovery();
			new Thread( instance::listener ).start();
		}
		return instance;
	}

	/**
	 * Continuously announces a service given its name and uri
	 * 
	 * @param serviceName the composite service name: <domain:service>
	 * @param serviceURI - the uri of the service
	 */
	public void announce(String serviceName, String serviceURI) {
		Log.info(String.format("Starting Discovery announcements on: %s for: %s -> %s\n", DISCOVERY_ADDR, serviceName, serviceURI));

		byte[] pktBytes = String.format("%s%s%s", serviceName, DELIMITER, serviceURI).getBytes();

		DatagramPacket pkt = new DatagramPacket(pktBytes, pktBytes.length, DISCOVERY_ADDR);
		new Thread(() -> {
			try (DatagramSocket ds = new DatagramSocket()) {
				for (;;) {
					ds.send(pkt);
					Thread.sleep(DISCOVERY_PERIOD);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}

	/**
	 * Listens for the given composite service name, blocks until a minimum number of replies is collected.
	 * @param serviceName - the composite name of the service
	 * @param minRepliesNeeded - the minimum number of replies required.
	 * @return the discovery results as an array
	 */
	
	public void listener() {
		Log.info(String.format("Starting discovery on multicast group: %s, port: %d\n", DISCOVERY_ADDR.getAddress(), DISCOVERY_ADDR.getPort()));

		final int MAX_DATAGRAM_SIZE = 65535;

		var pkt = new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);

		try (var ms = new MulticastSocket(DISCOVERY_ADDR.getPort())) {
			joinGroupInAllInterfaces(ms);
			for(;;) {
				try {
					pkt.setLength(MAX_DATAGRAM_SIZE);
					ms.receive(pkt);
					
					var tokens = new String(pkt.getData(), 0, pkt.getLength()).split(DELIMITER);
					Log.finest( "Received: " + Arrays.asList(tokens) + "\n");
					
					if (tokens.length == 2) {
						
						var name = tokens[0];
						var uri = URI.create( tokens[1]);
						
						discoveries.computeIfAbsent(name, (k) -> ConcurrentHashMap.newKeySet()).add( uri );
					}
				} catch (IOException e) {
					Sleep.ms(DISCOVERY_PERIOD);
					Log.finest("Still listening...");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public URI[] findUrisOf(String serviceName, int minRepliesNeeded) {
		Log.info(String.format("Discovery.findUrisOf( serviceName: %s, minRequired: %d\n", serviceName, minRepliesNeeded));
		
		for(;;) {
			var results = discoveries.get( serviceName );
			if( results != null && results.size() >= minRepliesNeeded )
				return results.toArray( new URI[ results.size() ]);
			else
				Sleep.ms( DISCOVERY_PERIOD );
		}
	}

	static private void joinGroupInAllInterfaces(MulticastSocket ms) throws SocketException {
		Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
		while (ifs.hasMoreElements()) {
			NetworkInterface xface = ifs.nextElement();
			try {
				ms.joinGroup(DISCOVERY_ADDR, xface);
			} catch (Exception x) {
				x.printStackTrace();
			}
		}
	}	
}
