package tp1.impl.servers.soap;

import java.util.function.Function;

import tp1.api.service.java.Result;

public class SoapWebService {

	/*
	 * Given a Result<T> returns T value or throws an exception created
	 * using the given function 
	 */
	<T, E extends Throwable> T resultOrThrow(Result<T> result, Function<String, E> exceptionMapper) throws E {
		if (result.isOK())
			return result.value();
		else
			throw exceptionMapper.apply( result.error().toString() );

	}
}
