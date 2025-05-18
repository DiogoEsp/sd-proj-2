package fctreddit.impl.client.rest;

import java.net.URI;
import java.util.logging.Logger;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import fctreddit.api.java.Result;
import fctreddit.api.rest.RestImage;
import fctreddit.impl.client.ImageClient;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

public class RestImageClient extends ImageClient {

	private static Logger Log = Logger.getLogger(RestImageClient.class.getName());

	final Client client;
	final ClientConfig config;

	final WebTarget target;
	
	public RestImageClient( URI serverURI ) {
		super(serverURI);

		this.config = new ClientConfig();
		
		config.property( ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
		config.property( ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

		
		this.client = ClientBuilder.newClient(config);

		target = client.target( serverURI ).path( RestImage.PATH );
	}
	
	@Override
	public Result<String> createImage(String userId, byte[] imageContents, String password) {
		Log.info("executing create Image: " + userId + " " + imageContents.length + " bytes of image");
		
		Response r = this.executePost(this.target.path(userId).queryParam(RestImage.PASSWORD, password)
				.request().accept(MediaType.APPLICATION_JSON), Entity.entity(imageContents, MediaType.APPLICATION_OCTET_STREAM));
		
		return this.extractResponseWithBody(r, String.class);
	}

	@Override
	public Result<byte[]> getImage(String userId, String imageId) {
		Log.info("executing get Image: " + userId + " " + imageId);
		
		Response r = this.executeGet(this.target.path(userId).path(imageId).request()
				.accept(MediaType.APPLICATION_OCTET_STREAM));
		
		return this.extractResponseWithBody(r, byte[].class);
	}

	@Override
	public Result<Void> deleteImage(String userId, String imageId, String password) {
		Log.info("executing delete Image: " + userId + " " + imageId);
		
		Response r = this.executeDelete(this.target.path(userId).path(imageId)
				.queryParam(RestImage.PASSWORD, password).request());
		
		return this.extractResponseWithoutBody(r);
	}

}
