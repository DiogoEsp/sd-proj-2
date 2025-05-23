package fctreddit.api.imgur.ResponseData;
import java.util.List;

public class GetAlbumData {
    public ImgurAlbum data;
    public boolean success;
    public int status;
}

class ImgurAlbum {
    public String id;
    public String title;
    public String description;
    public long datetime;
    public String cover;
    public int cover_width;
    public int cover_height;
    public String account_url;
    public Long account_id;
    public String privacy;
    public String layout;
    public int views;
    public String link;
    public boolean favorite;
    public boolean nsfw;
    public String section;
    public int images_count;
    public boolean in_gallery;
    public boolean is_ad;
    public boolean include_album_ads;
    public List<ImgurImage> images;
}

class ImgurImage {
    public String id;
    public String title;
    public String description;
    public long datetime;
    public String type;
    public boolean animated;
    public int width;
    public int height;
    public int size;
    public int views;
    public long bandwidth;
    public String link;
}
