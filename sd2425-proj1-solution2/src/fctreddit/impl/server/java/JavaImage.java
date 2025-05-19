package fctreddit.impl.server.java;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import com.github.scribejava.apis.ImgurApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import fctreddit.api.User;
import fctreddit.api.imgur.data.CreateAlbumArguments;
import fctreddit.api.java.Image;
import fctreddit.api.java.Result;
import fctreddit.api.java.Result.ErrorCode;
import fctreddit.api.imgur.*;

public class JavaImage extends JavaServer implements Image {

	private static final String apiKey = "7acbc7e0d5ce8fa";
	private static final String apiSecret = "e6c579220e16ceee0ff776b336a3b63a4a0c4c96";
	private static final String accessTokenStr = "97847fa95a42a56a0bd792fcb4273f16a27fc871";

	private static final String CREATE_ALBUM_URL = "https://api.imgur.com/3/album";
	private static final String UPLOAD_IMAGE_URL = "https://api.imgur.com/3/image";
	private static final String ADD_IMAGE_TO_ALBUM_URL = "https://api.imgur.com/3/album/{{albumHash}}/add";
	private static final String GET_ALBUM_URL = "https://api.imgur.com/3/album/{{albumHash}}";
	private static final String GET_IMAGE_URL = "https://api.imgur.com/3/image/{{imageId}}";
	private static final int HTTP_SUCCESS = 200;
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
	private static final String ALBUM_NAME = "SD";

	private final Gson json;
	private final OAuth20Service service;
	private final OAuth2AccessToken accessToken;

	private static Logger Log = Logger.getLogger(JavaImage.class.getName());
	
	private static final Path baseDirectory = Path.of("home", "sd", "images");

	public JavaImage() {
		json = new Gson();
		accessToken = new OAuth2AccessToken(accessTokenStr);
		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(ImgurApi.instance());

		//n preciso
		File f = baseDirectory.toFile();

		if (!f.exists()) {
			f.mkdirs();
		}
	}

	@Override
	public Result<String> createImage(String userId, byte[] imageContents, String password) throws IOException, ExecutionException, InterruptedException {

		Result<User> owner = getUsersClient().getUser(userId, password);

		if (!owner.isOK())
			return Result.error(owner.error());

		String id = null;

		//get do album
		String requestURL = ADD_IMAGE_TO_ALBUM_URL.replaceAll("\\{\\{albumHash\\}\\}",ALBUM_NAME);
		OAuthRequest request = new OAuthRequest(Verb.GET, requestURL);
		request.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
		service.signRequest(accessToken, request);
		Response response = service.execute(request);

		//se o album n existir fazê-lo
		if(response.getCode() != HTTP_SUCCESS){
			request = new OAuthRequest(Verb.POST, CREATE_ALBUM_URL);

			request.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
			request.setPayload(json.toJson(new CreateAlbumArguments(ALBUM_NAME, ALBUM_NAME)));

			service.signRequest(accessToken, request);
			try {
				service.execute(request);
			}
			catch (InterruptedException | ExecutionException | IOException e) {
				e.printStackTrace();
				return Result.error(ErrorCode.INTERNAL_ERROR);
			}
		}



		// check if user directory exists
		Path userDirectory = Path.of(baseDirectory.toString(), userId);
		File uDir = userDirectory.toFile();
		if (!uDir.exists()) {
			uDir.mkdirs();
		}

		synchronized (this) {
			while (true) {
				id = UUID.randomUUID().toString();
				image = Path.of(userDirectory.toString(), id);
				File iFile = image.toFile();

				if (!iFile.exists())
					break;
			}

			try {
				Files.write(image, imageContents);
			} catch (IOException e) {
				e.printStackTrace();
				return Result.error(ErrorCode.INTERNAL_ERROR);
			}
		}
		
		Log.info("Created image with id " + id + " for user " + userId);
		
		return Result.ok(id);
	}

	@Override
	public Result<byte[]> getImage(String userId, String imageId) {
		Log.info("Get image with id " + imageId + " owned by user " + userId);
		
		Path image = Path.of(baseDirectory.toString(), userId, imageId);
		File iFile = image.toFile();

		synchronized (this) {
			if (iFile.exists() && iFile.isFile()) {
				try {
					return Result.ok(Files.readAllBytes(image));
				} catch (IOException e) {
					e.printStackTrace();
					return Result.error(ErrorCode.INTERNAL_ERROR);
				}
			} else {
				return Result.error(ErrorCode.NOT_FOUND);
			}
		}
		
	}

	@Override
	public Result<Void> deleteImage(String userId, String imageId, String password) {
		Log.info("Delete image with id " + imageId + " owned by user " + userId);
		
		Result<User> owner = getUsersClient().getUser(userId, password);

		if (!owner.isOK()) {
			Log.info("Failed to authenticate user: " + owner.error());
			return Result.error(owner.error());
		}

		Path image = Path.of(baseDirectory.toString(), userId, imageId);
		File iFile = image.toFile();

		synchronized (this) {
			if (iFile.exists() && iFile.isFile()) {
				iFile.delete();
				return Result.ok();
			} else {
				return Result.error(ErrorCode.NOT_FOUND);
			}
		}
	}

}
