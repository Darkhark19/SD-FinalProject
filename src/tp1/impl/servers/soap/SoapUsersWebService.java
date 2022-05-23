package tp1.impl.servers.soap;

import java.util.List;
import java.util.logging.Logger;

import jakarta.jws.WebService;
import tp1.api.User;
import tp1.api.service.java.Users;
import tp1.api.service.soap.SoapUsers;
import tp1.api.service.soap.UsersException;
import tp1.impl.servers.common.JavaUsers;

@WebService(serviceName=SoapUsers.NAME, targetNamespace=SoapUsers.NAMESPACE, endpointInterface=SoapUsers.INTERFACE)
public class SoapUsersWebService extends SoapWebService implements SoapUsers {

	static Logger Log = Logger.getLogger(SoapUsersWebService.class.getName());

	final Users impl ;
	
	public SoapUsersWebService() {
		impl = new JavaUsers();
	}

	@Override
	public String createUser(User user) throws UsersException {
		Log.info(String.format("SOAP createUser: user = %s\n", user));

		return super.resultOrThrow( impl.createUser( user ), UsersException::new );
	}

	@Override
	public User getUser(String userId, String password) throws UsersException  {
		Log.info(String.format("SOAP getUser: userId = %s password=%s\n", userId, password));

		return super.resultOrThrow( impl.getUser(userId, password), UsersException::new );
	}


	@Override
	public User updateUser(String userId, String password, User user) throws UsersException  {
		Log.info(String.format("SOAP updateUser: userId = %s, user = %s\n", userId, user));

		return super.resultOrThrow( impl.updateUser(userId, password, user), UsersException::new);
	}


	@Override
	public User deleteUser(String userId, String password) throws UsersException  {
		Log.info(String.format("SOAP deleteUser: userId = %s\n", userId));
		
		return super.resultOrThrow( impl.deleteUser(userId, password), UsersException::new );
	}

	
	@Override
	public List<User> searchUsers(String pattern) throws UsersException  {
		Log.info(String.format("SOAP searchUsers: pattern = %s", pattern));
		
		return super.resultOrThrow( impl.searchUsers(pattern), UsersException::new );
	}
}
