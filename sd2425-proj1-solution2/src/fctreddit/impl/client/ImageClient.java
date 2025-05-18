package fctreddit.impl.client;

import java.net.URI;

import fctreddit.api.java.Image;
import fctreddit.impl.client.grpc.GrpcImageClient;
import fctreddit.impl.client.rest.RestImageClient;

public abstract class ImageClient extends Client implements Image {
		
	public ImageClient(URI serverURI) {
		super(serverURI);
	}

	public static ImageClient getImageClient(URI serverURI) {
		if(serverURI.toString().endsWith("/rest")) {
			return new RestImageClient(serverURI);
		} else {
			return new GrpcImageClient(serverURI);
		}
	}
	
}
