package fctreddit.impl.server.java;

import java.util.HashSet;
import java.util.Set;

import fctreddit.api.java.Content;
import fctreddit.api.java.Image;
import fctreddit.api.java.Users;
import fctreddit.impl.client.ContentClient;
import fctreddit.impl.client.ImageClient;
import fctreddit.impl.client.UsersClient;
import fctreddit.impl.server.Discovery;

public abstract class JavaServer {

	private static Discovery discovery;
	
	private Set<Content> contentClients;

	private Set<Image> imageClients;
	
	private Set<Users> usersClients;
	
	public static void setDiscovery(Discovery d) {
		if (JavaServer.discovery == null)
			JavaServer.discovery = d;
	}

	protected Content getContentClient() {
		synchronized (this.contentClients) {
			if (this.contentClients.isEmpty()) {
				this.contentClients.add(ContentClient.getContentClient(JavaServer.discovery.knownUrisOf("Content", 1)[0]));
			}
		}
		return this.contentClients.iterator().next();
	}

	protected Image getImageClient() {
		synchronized (this.imageClients) {
			if (this.imageClients.isEmpty()) {
				this.imageClients.add(ImageClient.getImageClient(JavaServer.discovery.knownUrisOf("Image", 1)[0]));
			}
		}	
		return this.imageClients.iterator().next();
	}
	
	protected Users getUsersClient() {
		synchronized (this.usersClients) {
			if (this.usersClients.isEmpty()) {
				this.usersClients.add(UsersClient.getUsersClient(JavaServer.discovery.knownUrisOf("Users", 1)[0]));
			}
		}
		return this.usersClients.iterator().next();
	}
	
	public JavaServer() {
		this.contentClients = new HashSet<Content>();
		this.imageClients = new HashSet<Image>();
		this.usersClients = new HashSet<Users>();
	}
	
	public String extractResourceID(String url) {
		return url.substring(url.lastIndexOf('/') + 1);
	}
	
}
