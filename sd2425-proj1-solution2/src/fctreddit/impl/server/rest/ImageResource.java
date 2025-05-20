package fctreddit.impl.server.rest;

import fctreddit.api.java.Image;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestImage;
import fctreddit.impl.server.java.JavaImage;
import jakarta.ws.rs.WebApplicationException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ImageResource extends RestResource implements RestImage {

	Image impl;
	
	private static String baseURI = null;

	
	public ImageResource() {
		impl = new JavaImage();
	}
	
	public static void setServerBaseURI(String s) {
		if(ImageResource.baseURI == null)
			ImageResource.baseURI = s;
	}
	
	@Override
	public String createImage(String userId, byte[] imageContents, String password){
		Result<String> res = null;
		try {
			res = impl.createImage(userId, imageContents, password);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		if(res.isOK())
			return ImageResource.baseURI + RestImage.PATH + "/" + userId + "/" + res.value();
		
		throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public byte[] getImage(String userId, String imageId) {
		Result<byte[]> res = impl.getImage(userId, imageId);
		
		if(res.isOK())
			return res.value();
		
		throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public void deleteImage(String userId, String imageId, String password) {
		Result<Void> res = impl.deleteImage(userId, imageId, password);
		
		if(res.isOK())
			return;
		
		throw new WebApplicationException(errorCodeToStatus(res.error()));
	}
}
