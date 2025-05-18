package fctreddit.api.imgur.data;

import java.util.Base64;

public record ImageUploadArguments(String image, 
		String type, 
		String title, 
		String description) {

	public ImageUploadArguments(byte[] image, String title) {
		this(Base64.getEncoder().encodeToString(image), "base64", title, title);
	}
	
	public byte[] getImageData() {
		return Base64.getDecoder().decode(this.image);
	}
}