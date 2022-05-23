package tp1.impl.clients.soap;

import static tp1.api.service.java.Result.error;
import static tp1.api.service.java.Result.ok;

import java.net.URI;
import java.util.function.Supplier;

import com.sun.xml.ws.client.BindingProviderProperties;

import jakarta.xml.ws.BindingProvider;
import jakarta.xml.ws.WebServiceException;
import tp1.api.service.java.Result;
import tp1.api.service.java.Result.ErrorCode;
import tp1.impl.clients.common.RetryClient;
import tp1.tls.InsecureHostnameVerifier;

import javax.net.ssl.HttpsURLConnection;

/**
* 
* Shared behavior among SOAP clients.
* 
* Holds endpoint information.
* 
* Translates soap responses/exceptions to Result<T> for interoperability.
*  
* @author smduarte
*
*/
abstract class SoapClient<T> extends RetryClient {
	
	protected static final String WSDL = "?wsdl";

	protected final URI uri;
	protected final T impl;
	
	public SoapClient(URI uri, Supplier<T> func) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		this.uri = uri;
		this.impl = func.get();
		this.setTimeouts((BindingProvider) impl);
	}

	private void setTimeouts(BindingProvider port ) {
		port.getRequestContext().put(BindingProviderProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
		port.getRequestContext().put(BindingProviderProperties.REQUEST_TIMEOUT, READ_TIMEOUT);		
	}

	
	

	protected <R> Result<R> toJavaResult(ResultSupplier<R> supplier) {
		try {
			return ok( supplier.get());	
		} 
		catch (Exception e) {			
			if( e instanceof WebServiceException ) {
				throw new RuntimeException( e.getMessage() );
			}			
			return error(getErrorCodeFrom(e));
		}
	}

	protected <R> Result<R> toJavaResult( VoidSupplier r) {
		try {
			r.run();
			return ok();
		}
		catch (Exception e) {
			if( e instanceof WebServiceException ) {
				throw new RuntimeException( e.getMessage() );				
			}
			return error(getErrorCodeFrom(e));
		}
	}

	static private ErrorCode getErrorCodeFrom(Exception e) {
		try {
			return ErrorCode.valueOf( e.getMessage() );			
		} catch( IllegalArgumentException x) {			
			return ErrorCode.INTERNAL_ERROR ;			
		}
	}

	static interface ResultSupplier<T> {
		T get() throws Exception;
	}

	static interface VoidSupplier {
		void run() throws Exception;
	}
	
	@Override
	public String toString() {
		return uri.toString();
	}	
}
