package tp1.impl.servers.rest;

import java.util.List;
import java.util.logging.Logger;

import jakarta.inject.Singleton;
import tp1.api.User;
import tp1.api.service.java.Users;
import tp1.api.service.rest.RestUsers;
import tp1.impl.servers.common.JavaUsers;

@Singleton
public class UsersResources extends RestResource implements RestUsers {
	private static Logger Log = Logger.getLogger(UsersResources.class.getName());

	final Users impl ;
	static UsersResources instance;
	
	public UsersResources() {
		impl = new JavaUsers();
		instance = this;
	}
	
	@Override
	public String createUser(User user) {
		Log.info(String.format("REST createUser: user = %s\n", user));

		return resultOrThrow( impl.createUser( user ));
	}

	@Override
	public User getUser(String userId, String password) {
		Log.info(String.format("REST getUser: userId = %s password=%s\n", userId, password));

		return resultOrThrow( impl.getUser(userId, password));
	}


	@Override
	public User updateUser(String userId, String password, User user) {
		Log.info(String.format("REST updateUser: userId = %s, user = %s\n", userId, user));

		return resultOrThrow( impl.updateUser(userId, password, user));
	}


	@Override
	public User deleteUser(String userId, String password) {
		Log.info(String.format("REST deleteUser: userId = %s\n", userId));
		
		return resultOrThrow( impl.deleteUser(userId, password));
	}

	@Override
	public List<User> searchUsers(String pattern) {
		Log.info(String.format("REST searchUsers: pattern = %s", pattern));
		
		return resultOrThrow( impl.searchUsers(pattern));
	}
}
