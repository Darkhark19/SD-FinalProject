package tp1.impl.clients.rest;

import static tp1.api.service.java.Result.error;
import static tp1.api.service.java.Result.ok;

import java.net.URI;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tp1.api.service.java.Result;
import tp1.api.service.java.Result.ErrorCode;
import tp1.impl.clients.common.RetryClient;
import tp1.tls.InsecureHostnameVerifier;

import javax.net.ssl.HttpsURLConnection;

/**
 * 
 * Shared behavior among REST clients.
 * 
 * Holds client and target information.
 * 
 * Translates http responses to Result<T> for interoperability.
 * 
 * @author smduarte
 *
 */
abstract class RestClient extends RetryClient {

	protected final URI uri;
	protected final Client client;
	protected final WebTarget target;
	protected final ClientConfig config;

	public RestClient(URI uri, String path) {
		this.uri = uri;
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		this.config = new ClientConfig();
		this.config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
		this.config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
		this.config.property(ClientProperties.FOLLOW_REDIRECTS, true);

//		config.register(new LoggingFeature(Logger.getLogger(LoggingFeature.DEFAULT_LOGGER_NAME), Level.INFO, LoggingFeature.Verbosity.PAYLOAD_ANY, 10000));		  

		this.client = ClientBuilder.newClient(config);
		this.target = this.client.target(uri).path(path);
	}

	protected Result<Void> toJavaResult(Response r) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.NO_CONTENT)
				return ok();
			else
				return error(getErrorCodeFrom(status));
		} finally {
			r.close();
		}
	}

	protected <T> Result<T> toJavaResult(Response r, GenericType<T> gtype) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.OK)
				return ok(r.readEntity(gtype));
			else
				return error(getErrorCodeFrom(status));
		} finally {
			r.close();
		}
	}

	static private ErrorCode getErrorCodeFrom(Status status) {
		return switch (status.getStatusCode()) {
		case 200, 209 -> ErrorCode.OK;
		case 409 -> ErrorCode.CONFLICT;
		case 403 -> ErrorCode.FORBIDDEN;
		case 404 -> ErrorCode.NOT_FOUND;
		case 400 -> ErrorCode.BAD_REQUEST;
		case 500 -> ErrorCode.INTERNAL_ERROR;
		case 501 -> ErrorCode.NOT_IMPLEMENTED;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}

	@Override
	public String toString() {
		return uri.toString();
	}
}
