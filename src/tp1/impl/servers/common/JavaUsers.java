package tp1.impl.servers.common;

import static tp1.api.service.java.Result.error;
import static tp1.api.service.java.Result.ok;
import static tp1.api.service.java.Result.ErrorCode.BAD_REQUEST;
import static tp1.api.service.java.Result.ErrorCode.CONFLICT;
import static tp1.api.service.java.Result.ErrorCode.FORBIDDEN;
import static tp1.api.service.java.Result.ErrorCode.NOT_FOUND;
import static tp1.impl.clients.Clients.DirectoryClients;
import static tp1.impl.clients.Clients.FilesClients;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import tp1.api.User;
import tp1.api.service.java.Result;
import tp1.api.service.java.Users;
import util.Token;

public class JavaUsers implements Users {
	final protected Map<String, User> users = new ConcurrentHashMap<>();
	final ExecutorService executor = Executors.newCachedThreadPool();
	
	@Override
	public Result<String> createUser(User user) {
		if( badUser(user ))
			return error( BAD_REQUEST );
		
		var userId = user.getUserId();
		var res = users.putIfAbsent(userId, user);
		
		if (res != null)
			return error(CONFLICT);
		else
			return ok(userId);
	}

	@Override
	public Result<User> getUser(String userId, String password) {
		if (badParam(userId) )
			return error(BAD_REQUEST);
		
		var user = users.get(userId);
		
		if (user == null)
			return error(NOT_FOUND);
		
		if (badParam(password) || wrongPassword(user, password))
			return error(FORBIDDEN);
		else
			return ok(user);
	}

	@Override
	public Result<User> updateUser(String userId, String password, User data) {

		var user = users.get(userId);
		
		if (user == null)
			return error(NOT_FOUND);
		
		if (badParam(password) || wrongPassword(user, password))
			return error(FORBIDDEN);
		else {
			user.updateUser(data);
			return ok(user);
		}
	}

	@Override
	public Result<User> deleteUser(String userId, String password) {
		
		var user = users.get(userId);
		
		if (user == null)
			return error(NOT_FOUND);
		
		if (badParam(password) || wrongPassword(user, password))
			return error(FORBIDDEN);
		else {
			users.remove(userId);
			executor.execute(()->{
				DirectoryClients.get().deleteUserFiles(userId, password, Token.get());
				for( var uri : FilesClients.all())
					FilesClients.get(uri).deleteUserFiles( userId, password);
			});
			return ok(user);
		}
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		if( badParam( pattern))
			return error(BAD_REQUEST);
					
		var hits = users.values()
			.stream()
			.filter( u -> u.getFullName().toLowerCase().contains(pattern.toLowerCase()) )
			.map( User::secureCopy )
			.toList();
		
		return ok(hits);
	}
	
	private boolean badParam( String str ) {
		return str == null;
	}
	
	private boolean badUser( User user ) {
		return user == null || badParam(user.getEmail()) || badParam(user.getFullName()) || badParam( user.getPassword());
	}
	
	private boolean wrongPassword(User user, String password) {
		return !user.getPassword().equals(password);
	}
}
