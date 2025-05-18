package fctreddit.impl.client;

import java.net.URI;

import fctreddit.api.java.Content;
import fctreddit.impl.client.grpc.GrpcContentClient;
import fctreddit.impl.client.rest.RestContentClient;

public abstract class ContentClient extends Client implements Content {
	
	public ContentClient(URI serverURI) {
		super(serverURI);
	}

	public static ContentClient getContentClient(URI serverURI) {
		if(serverURI.toString().endsWith("/rest")) {
			return new RestContentClient(serverURI);
		} else {
			return new GrpcContentClient(serverURI);
		}
	}
	
}
