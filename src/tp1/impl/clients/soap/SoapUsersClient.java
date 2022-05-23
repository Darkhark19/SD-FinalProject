package tp1.impl.clients.soap;

import java.net.URI;
import java.util.List;

import javax.xml.namespace.QName;

import jakarta.xml.ws.Service;
import tp1.api.User;
import tp1.api.service.java.Result;
import tp1.api.service.java.Users;
import tp1.api.service.soap.SoapUsers;
import util.Url;

public class SoapUsersClient extends SoapClient<SoapUsers> implements Users {
	
	public SoapUsersClient( URI serverUri ) {
		super( serverUri, () -> {
			QName QNAME = new QName(SoapUsers.NAMESPACE, SoapUsers.NAME);			
			Service service = Service.create(Url.from(serverUri + WSDL), QNAME);
			return service.getPort(tp1.api.service.soap.SoapUsers.class);
		} );
	}
	
	@Override
	public Result<String> createUser(User user) {
		return super.toJavaResult(() -> impl.createUser(user));
	}

	@Override
	public Result<User> getUser(String userId, String password) {
		return super.toJavaResult(() -> impl.getUser(userId, password));
	}

	@Override
	public Result<User> updateUser(String userId, String password, User user) {
		return super.toJavaResult(() -> impl.updateUser(userId, password, user));
	}

	@Override
	public Result<User> deleteUser(String userId, String password) {
		return super.toJavaResult(() -> impl.deleteUser(userId, password));
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		return super.toJavaResult(() -> impl.searchUsers(pattern));
	}
}
