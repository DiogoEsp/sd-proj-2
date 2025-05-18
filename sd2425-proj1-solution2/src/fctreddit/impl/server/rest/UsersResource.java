package fctreddit.impl.server.rest;

import java.util.List;
import java.util.logging.Logger;

import fctreddit.api.User;
import fctreddit.api.java.Result;
import fctreddit.api.java.Users;
import fctreddit.api.rest.RestUsers;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.java.JavaUsers;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;

public class UsersResource extends RestResource implements RestUsers {

	private static Logger Log = Logger.getLogger(UsersResource.class.getName());
	
	private Hibernate hibernate;
	Users impl;
	
	public UsersResource() {
		hibernate = Hibernate.getInstance();
		impl = new JavaUsers();
	}
	
	@Override
	public String createUser(User user) {
		Log.info("createUser : " + user);
		
		Result<String> res = impl.createUser(user);
		if(!res.isOK()) {
			throw new WebApplicationException(errorCodeToStatus(res.error()));
		}
		return res.value();
	}

	@Override
	public User getUser(String userId, String password) {
		Log.info("getUser : user = " + userId + "; pwd = " + password);

		Result<User> res = impl.getUser(userId, password);
		if(!res.isOK()) {
			throw new WebApplicationException(errorCodeToStatus(res.error()));
		}
		return res.value();
	}

	@Override
	public User updateUser(String userId, String password, User user) {
		Log.info("updateUser : user = " + userId + "; pwd = " + password + " ; userData = " + user);
		
		Result<User> res = impl.updateUser(userId, password, user);
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
		
		return res.value();
	}

	@Override
	public User deleteUser(String userId, String password) {
		Log.info("deleteUser : user = " + userId + "; pwd = " + password);
		
		Result<User> res = impl.deleteUser(userId, password);
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
		
		return res.value();
	}

	@Override
	public List<User> searchUsers(String pattern) {
		Log.info("searchUsers : pattern = " + pattern);
		
		try {
			List<User> list = hibernate.jpql("SELECT u FROM User u WHERE u.userId LIKE '%" + pattern +"%'", User.class);
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
		}
	}
}
