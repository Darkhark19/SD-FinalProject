package tp1.impl.clients.common;

import tp1.api.service.java.Files;
import tp1.api.service.java.Result;

public class RetryFilesClient extends RetryClient implements Files {

	final Files impl;

	public RetryFilesClient( Files impl ) {
		this.impl = impl;	
	}

	@Override
	public Result<byte[]> getFile(String fileId, String token) {
		return reTry( () -> impl.getFile(fileId, token));
	}

	@Override
	public Result<Void> deleteFile(String fileId, String token) {
		return reTry( () -> impl.deleteFile(fileId, token));
	}

	@Override
	public Result<Void> writeFile(String fileId, byte[] data, String token) {
		// We do not retry this operation more than once, here...
		// In case of timeout, directory needs to try another server instead.
		return reTry( () -> impl.writeFile(fileId, data, token), 1);
	}

	@Override
	public Result<Void> deleteUserFiles(String userId, String token) {
		return reTry( () -> impl.deleteUserFiles(userId, token));
	}	
}
