package fctreddit.api.imgur;

import com.github.scribejava.apis.ImgurApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import fctreddit.api.imgur.data.AddImagesToAlbumArguments;
import fctreddit.api.imgur.data.BooleanBasicResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class AddImageToAlbum {

	private static final String apiKey = "7acbc7e0d5ce8fa";
	private static final String apiSecret = "e6c579220e16ceee0ff776b336a3b63a4a0c4c96";
	private static final String accessTokenStr = "97847fa95a42a56a0bd792fcb4273f16a27fc871";
	
	private static final String ADD_IMAGE_TO_ALBUM_URL = "https://api.imgur.com/3/album/{{albumHash}}/add";
		
	private static final int HTTP_SUCCESS = 200;
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
	
	private final Gson json;
	private final OAuth20Service service;
	private final OAuth2AccessToken accessToken;
	
	public AddImageToAlbum() {
		json = new Gson();
		accessToken = new OAuth2AccessToken(accessTokenStr);
		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(ImgurApi.instance());
	}
	
	public boolean execute(String albumId, String imageId) {
		String requestURL = ADD_IMAGE_TO_ALBUM_URL.replaceAll("\\{\\{albumHash\\}\\}", albumId);
		
		OAuthRequest request = new OAuthRequest(Verb.POST, requestURL);
		
		request.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
		request.setPayload(json.toJson(new AddImagesToAlbumArguments(imageId)));
		
		service.signRequest(accessToken, request);
		
		try {
			Response r = service.execute(request);
			
			
			if(r.getCode() != HTTP_SUCCESS) {
				//Operation failed
				System.err.println("Operation Failed\nStatus: " + r.getCode() + "\nBody: " + r.getBody());
				return false;
			} else {
				System.err.println("Contents of Body: " + r.getBody());
				BooleanBasicResponse body = json.fromJson(r.getBody(), BooleanBasicResponse.class);
				System.out.println("Operation Succedded");			
				return body.isSuccess();
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (ExecutionException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return false;
	}
	
	public static void main(String[] args) throws Exception {
	
		if( args.length != 2 ) {
			System.err.println("usage: java " + AddImageToAlbum.class.getCanonicalName() +  " <album-id> <image-id>");
			System.exit(0);
		}	
		
		String albumId = args[0];
		String imageId = args[1];
		AddImageToAlbum ca = new AddImageToAlbum();
		
		if(ca.execute(albumId, imageId))
			System.out.println("Added " + imageId + " to album " + albumId + " successfuly.");
		else
			System.err.println("Failed to execute operation");
	}
	
}