package fctreddit.impl.client.rest;

import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import fctreddit.api.User;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestUsers;
import fctreddit.impl.client.UsersClient;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

public class RestUsersClient extends UsersClient {
	
	private static Logger Log = Logger.getLogger(RestUsersClient.class.getName());

	final Client client;
	final ClientConfig config;

	final WebTarget target;
	
	public RestUsersClient( URI serverURI ) {
		super(serverURI);

		this.config = new ClientConfig();
		
		config.property( ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
		config.property( ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

		
		this.client = ClientBuilder.newClient(config);

		target = client.target( serverURI ).path( RestUsers.PATH );
	}
		
	public Result<String> createUser(User user) {
		Log.info("Creating user: " + user.toString());
		
		Response r = this.executePost(target.request().accept( MediaType.APPLICATION_JSON),
				Entity.entity(user, MediaType.APPLICATION_JSON));
		
		return extractResponseWithBody(r, String.class);
	}

	public Result<User> getUser(String userId, String pwd) {
		Log.info("Getting user: " + userId);
		
		Response r = this.executeGet(target.path(userId).queryParam(RestUsers.PASSWORD, pwd)
				.request().accept(MediaType.APPLICATION_JSON));
		
		return this.extractResponseWithBody(r, User.class);
	}
	
	

	public Result<User> updateUser(String userId, String password, User user) {
		Log.info("Updating user: " + userId + " " + user.toString());
		
		Response r = this.executePut(target.path(userId).queryParam(RestUsers.PASSWORD, password)
				.request().accept(MediaType.APPLICATION_JSON), Entity.entity(user, MediaType.APPLICATION_JSON));
		
		return this.extractResponseWithBody(r, User.class);
	}

	public Result<User> deleteUser(String userId, String password) {
		Log.info("Deleting user: " + userId);
		
		Response r = this.executeDelete(target.path(userId).queryParam(RestUsers.PASSWORD, password)
				.request().accept(MediaType.APPLICATION_JSON));
		
		return this.extractResponseWithBody(r, User.class);
	}

	public Result<List<User>> searchUsers(String pattern) {
		Log.info("Searching user: " + pattern);
		
		Response r = this.executeGet(target.queryParam(RestUsers.QUERY, pattern).request()
				.accept(MediaType.APPLICATION_JSON));
		
		return this.extractResponseWithBody(r, new GenericType<List<User>>() {});
	}

}
