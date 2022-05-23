package tp1.impl.servers.soap;

import java.util.logging.Logger;

import jakarta.jws.WebService;
import tp1.api.service.java.Files;
import tp1.api.service.soap.FilesException;
import tp1.api.service.soap.SoapFiles;
import tp1.impl.servers.common.JavaFiles;
import tp1.impl.servers.common.dropbox.DropBoxFiles;

@WebService(serviceName = SoapFiles.NAME, targetNamespace = SoapFiles.NAMESPACE, endpointInterface = SoapFiles.INTERFACE)
public class SoapFilesWebService extends SoapWebService implements SoapFiles {

	private static Logger Log = Logger.getLogger(SoapFilesWebService.class.getName());

	final Files impl ;
	
	public SoapFilesWebService() {
		impl = new JavaFiles();
	}


	@Override
	public void writeFile(String fileId, byte[] data, String token) throws FilesException {
		Log.info(String.format("SOAP writeFile: fileId = %s, data.length = %d, token = %s \n", fileId, data.length, token));

		super.resultOrThrow( impl.writeFile(fileId, data, token), FilesException::new );
	}
	
	@Override
	public void deleteFile(String fileId, String token) throws FilesException {
		Log.info(String.format("SOAP deleteFile: fileId = %s, token = %s \n", fileId, token));

		super.resultOrThrow( impl.deleteFile(fileId, token), FilesException::new);
	}
	
	@Override
	public byte[] getFile(String fileId, String token) throws FilesException {
		Log.info(String.format("SOAP getFile: fileId = %s,  token = %s \n", fileId, token));

		return super.resultOrThrow( impl.getFile(fileId, token), FilesException::new);
	}

	@Override
	public void deleteUserFiles(String userId, String token) throws FilesException {
		Log.info(String.format("SOAP deleteUserFiles: userId = %s, token = %s \n", userId, token));

		super.resultOrThrow( impl.deleteUserFiles(userId, token), FilesException::new);
	}
}
