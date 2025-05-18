package fctreddit.api.imgur.data;

public record CreateAlbumArguments(String title, 
		String description, 
		String privacy, 
		String layout, 
		String cover, 
		String[] ids, 
		String[] deletedhashes) {
	
	public CreateAlbumArguments(String title, String description) {
		this(title, description, "public", "grid", null, null, null);
	}
	
}