package tp1.impl.clients;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import tp1.api.service.java.Result.ErrorCode;
import tp1.impl.discovery.Discovery;

public class ClientFactory<T> {

	private static final String REST = "/rest";
	private static final String SOAP = "/soap";

	private final String serviceName;
	private final Function<URI, T> restClient;
	private final Function<URI, T> soapClient;
	
	ClientFactory( String serviceName, Function<URI, T> restClient, Function<URI, T> soapClient) {
		this.restClient = restClient;
		this.soapClient = soapClient;
		this.serviceName = serviceName;
	}
	
	
	LoadingCache<URI, T> clients = CacheBuilder.newBuilder()
			.build(new CacheLoader<>() {
				@Override
				public T load(URI uri) throws Exception {
					T client;
					if (uri.toString().endsWith(REST))
						client = restClient.apply( uri );
					else if (uri.toString().endsWith(SOAP))
						client = soapClient.apply( uri );
					else
						throw new RuntimeException("Unknown service type..." + uri);
					return client;
				}
			});
	
	
	public T get() {
		URI[] uris = Discovery.getInstance().findUrisOf(serviceName, 1);
		return get(uris[0]);
	}
	
	public T get(URI uri) {
		try {
			return clients.get(uri);
		} catch (Exception x) {
			x.printStackTrace();
			throw new RuntimeException( ErrorCode.INTERNAL_ERROR.toString());
		}
	}
	
	public T get(String urlString) {
		var i = urlString.indexOf(serviceName);			
		return this.get( URI.create( urlString.substring(0, i-1) ));
	}
	
	public List<URI> all()  {
		return Arrays.asList(Discovery.getInstance().findUrisOf(serviceName, 1));
	}	
}
