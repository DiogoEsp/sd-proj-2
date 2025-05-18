package fctreddit.api.imgur;

import com.github.scribejava.apis.ImgurApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import fctreddit.api.imgur.data.BasicResponse;
import fctreddit.api.imgur.data.ImageUploadArguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

public class ImageUpload {

	private static final String apiKey = "INSERT YOURS";
	private static final String apiSecret = "INSERT YOURS";
	private static final String accessTokenStr = "INSERT YOURS";
	
	private static final String UPLOAD_IMAGE_URL = "https://api.imgur.com/3/image";
		
	private static final int HTTP_SUCCESS = 200;
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
	
	private final Gson json;
	private final OAuth20Service service;
	private final OAuth2AccessToken accessToken;
	
	public ImageUpload() {
		json = new Gson();
		accessToken = new OAuth2AccessToken(accessTokenStr);
		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(ImgurApi.instance());
	}
	
	public boolean execute(String imageName, byte[] data) {
		OAuthRequest request = new OAuthRequest(Verb.POST, UPLOAD_IMAGE_URL);
		
		request.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
		request.setPayload(json.toJson(new ImageUploadArguments(data, imageName)));
		
		service.signRequest(accessToken, request);
		
		try {
			Response r = service.execute(request);
			
			if(r.getCode() != HTTP_SUCCESS) {
				//Operation failed
				System.err.println("Operation Failed\nStatus: " + r.getCode() + "\nBody: " + r.getBody());
				return false;
			} else {
				BasicResponse body = json.fromJson(r.getBody(), BasicResponse.class);
				System.out.println("Operation Succedded\nImage name: " + imageName + "\nImage ID: " + body.getData().get("id"));
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

		if( args.length != 1 ) {
			System.err.println("usage: java " + ImageUpload.class.getCanonicalName() + " <album-name>");
			System.exit(0);
		}
		
		String filename = args[0];
		
		byte[] data = null;
		
		try {
			data = Files.readAllBytes(Path.of("./", filename));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		ImageUpload ca = new ImageUpload();
		
		if(ca.execute(filename, data))
			System.out.println("Image '" + filename + "' uploaded successfuly.");
		else
			System.err.println("Failed to upload image from '" + filename + "'");
	}
	
}