package tp1.impl.clients.soap;

import java.net.URI;

import javax.xml.namespace.QName;

import jakarta.xml.ws.Service;
import tp1.api.service.java.Files;
import tp1.api.service.java.Result;
import tp1.api.service.soap.SoapFiles;
import util.Url;

public class SoapFilesClient extends SoapClient<SoapFiles> implements Files {

	public SoapFilesClient(URI serverURI) {
		super(serverURI, () -> {
			QName QNAME = new QName(SoapFiles.NAMESPACE, SoapFiles.NAME);
			Service service = Service.create(Url.from(serverURI + WSDL), QNAME);
			return service.getPort(tp1.api.service.soap.SoapFiles.class);			
		});
	}
	
	@Override
	public Result<byte[]> getFile(String fileId, String token) {
		return super.toJavaResult(() -> impl.getFile(fileId, token));
	}

	@Override
	public Result<Void> deleteFile(String fileId, String token) {
		return super.toJavaResult(() -> impl.deleteFile(fileId, token));
	}

	@Override
	public Result<Void> writeFile(String fileId, byte[] data, String token) {
		return super.toJavaResult(() -> impl.writeFile(fileId, data, token));
	}

	@Override
	public Result<Void> deleteUserFiles(String userId, String token) {
		return super.toJavaResult(() -> impl.deleteUserFiles(userId, token));
	}
}
