package tp1.api.service.java;

import java.util.List;

import tp1.api.User;

public interface Users {
	public static String SERVICE_NAME = "users";

	Result<String> createUser(User user);
	
	Result<User> getUser(String userId, String password);
	
	Result<User> updateUser(String userId, String password, User user);
	
	Result<User> deleteUser(String userId, String password);
	
	Result<List<User>> searchUsers(String pattern);	
}
