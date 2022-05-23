package tp1.impl.clients.common;

import java.util.List;

import tp1.api.User;
import tp1.api.service.java.Result;
import tp1.api.service.java.Users;


public class RetryUsersClient extends RetryClient implements Users {

	final Users impl;

	public RetryUsersClient( Users impl ) {
		this.impl = impl;	
	}

	@Override
	public Result<String> createUser(User user) {
		return reTry( () -> impl.createUser(user));
	}

	@Override
	public Result<User> getUser(String userId, String password) {
		return reTry( () -> impl.getUser(userId, password));
	}

	@Override
	public Result<User> deleteUser(String userId, String password) {
		return reTry( () -> impl.deleteUser(userId, password));		
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		return reTry( () -> impl.searchUsers(pattern));		
	}

	@Override
	public Result<User> updateUser(String userId, String password, User user) {
		return reTry( () -> impl.updateUser(userId, password, user));		
	}
}
