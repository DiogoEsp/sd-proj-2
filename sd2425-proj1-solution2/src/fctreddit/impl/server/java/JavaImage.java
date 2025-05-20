package fctreddit.impl.server.java;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.logging.Logger;
import fctreddit.api.User;
import fctreddit.api.java.Image;
import fctreddit.api.java.Result;
import fctreddit.api.java.Result.ErrorCode;

public class JavaImage extends JavaServer implements Image {


	private static Logger Log = Logger.getLogger(JavaImage.class.getName());

	private static final Path baseDirectory = Path.of("home", "sd", "images");

	public JavaImage() {
		File f = baseDirectory.toFile();

		if (!f.exists()) {
			f.mkdirs();
		}
	}

	@Override
	public Result<String> createImage(String userId, byte[] imageContents, String password) {
		System.out.println("arcadia0");
		Result<User> owner = getUsersClient().getUser(userId, password);
		System.out.println("arcadia1");
		if (!owner.isOK())
			return Result.error(owner.error());

		String id = null;
		Path image = null;

		// check if user directory exists
		Path userDirectory = Path.of(baseDirectory.toString(), userId);
		File uDir = userDirectory.toFile();
		if (!uDir.exists()) {
			uDir.mkdirs();
		}
		System.out.println("arcadia2");

		synchronized (this) {
			while (true) {
				id = UUID.randomUUID().toString();
				image = Path.of(userDirectory.toString(), id);
				File iFile = image.toFile();

				if (!iFile.exists())
					break;
			}
			System.out.println("arcadia3");
			try {
				Files.write(image, imageContents);
			} catch (IOException e) {
				e.printStackTrace();
				return Result.error(ErrorCode.INTERNAL_ERROR);
			}
		}

		Log.info("Created image with id " + id + " for user " + userId);
		System.out.println("arcadia4");
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
