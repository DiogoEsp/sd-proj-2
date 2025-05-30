package fctreddit.impl.server.java;

import com.github.scribejava.apis.ImgurApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import fctreddit.api.User;
import fctreddit.api.imgur.data.*;
import fctreddit.api.java.Image;
import fctreddit.api.java.Result;
import java.io.IOException;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import fctreddit.impl.client.UsersClient;

import java.util.List;
import java.util.concurrent.ExecutionException;
public class JavaImgur extends JavaServer implements Image {

    private static final String apiKey = "7acbc7e0d5ce8fa";
    private static final String apiSecret = "e6c579220e16ceee0ff776b336a3b63a4a0c4c96";
    private static final String accessTokenStr = "97847fa95a42a56a0bd792fcb4273f16a27fc871";

    private static final String CREATE_ALBUM_URL = "https://api.imgur.com/3/album";
    private static final String UPLOAD_IMAGE_URL = "https://api.imgur.com/3/image";
    private static final String ADD_IMAGE_TO_ALBUM_URL = "https://api.imgur.com/3/album/{{albumHash}}/add";
    private static final String GET_ALBUM_URL = "https://api.imgur.com/3/album/{{albumHash}}";
    private static final String GET_ALBUM_IMAGES_URL = "https://api.imgur.com/3/album/{{albumHash}}/images";
    private static final String GET_IMAGE_URL = "https://api.imgur.com/3/image/{{imageId}}";
    private static final String DELETE_IMAGE_URL = "https://api.imgur.com/3/image/{{imageDeleteHash}}";
    private static final int HTTP_SUCCESS = 200;
    private static final int HTTP_NOT_FOUND = 404;
    private static final String CONTENT_TYPE_HDR = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    private static final String APPLICATION_CONTENT_TYPE = "application/x-www-form-urlencoded";
    private final Gson json;
    private final OAuth20Service service;
    private final OAuth2AccessToken accessToken;
    private static String albumName;
    private String albumId;

    class AlbumListResponse {
        private List<Album> data;
        public List<Album> getData() { return data; }
    }

    // Album object
    class Album {
        private String id;
        private String title;
        public String getId() { return id; }
        public String getTitle() { return title; }
    }

    public JavaImgur(){
        json = new Gson();
        accessToken = new OAuth2AccessToken(accessTokenStr);
        service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(ImgurApi.instance());
    }

    @Override
    public Result<String> createImage(String userId, byte[] imageContents, String password) throws IOException, ExecutionException, InterruptedException {

        System.out.println("Arcadia1");
        Result<User> owner = getUsersClient().getUser(userId, password);

        if (!owner.isOK())
            return Result.error(owner.error());

        System.out.println("Arcadia2");

        if(albumId == null) {
            checkAlbum();
        }

        // Se não existir, criar o álbum
        if (albumId == null) {
            System.out.println("Arcadia4");
            OAuthRequest createAlbumReq = new OAuthRequest(Verb.POST, CREATE_ALBUM_URL);
            createAlbumReq.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            createAlbumReq.setPayload(json.toJson(new CreateAlbumArguments(albumName, albumName)));
            service.signRequest(accessToken, createAlbumReq);
            Response createAlbumResp = service.execute(createAlbumReq);

            if (createAlbumResp.getCode() != HTTP_SUCCESS) {
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            }

            BasicResponse albumResp = json.fromJson(createAlbumResp.getBody(), BasicResponse.class);
            albumId = albumResp.getData().get("id").toString();
        }

        System.out.println("Arcadia5");

        // 2. Fazer upload da imagem
        ImageUploadArguments uploadArgs = new ImageUploadArguments(imageContents, "imageTitle" );
        OAuthRequest uploadReq = new OAuthRequest(Verb.POST, UPLOAD_IMAGE_URL);
        uploadReq.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
        uploadReq.setPayload(json.toJson(uploadArgs));
        service.signRequest(accessToken, uploadReq);
        Response uploadResp = service.execute(uploadReq);

        if (uploadResp.getCode() != HTTP_SUCCESS) {
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        BasicResponse uploadResponse = json.fromJson(uploadResp.getBody(), BasicResponse.class);
        String imageId = uploadResponse.getData().get("id").toString();

        //3. Adicionar imagem ao album
        System.out.println("adding1");
        String request = ADD_IMAGE_TO_ALBUM_URL.replaceAll("\\{\\{albumHash\\}\\}", albumId);
        System.out.println("adding2");
        OAuthRequest addImageReq = new OAuthRequest(Verb.POST, request);
        System.out.println("adding3");
        addImageReq.addHeader(CONTENT_TYPE_HDR,JSON_CONTENT_TYPE);
        System.out.println("adding4");
        addImageReq.setPayload(json.toJson(new AddImagesToAlbumArguments(imageId)));
        System.out.println("adding5");
        service.signRequest(accessToken, addImageReq);
        System.out.println("adding6");
        Response addImageResp = service.execute(addImageReq);
        System.out.println("adding7");

        if (addImageResp.getCode() != HTTP_SUCCESS) {
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        // 4. Retornar o ID da imagem
        return Result.ok(imageId);
    }

    @Override
    public Result<byte[]> getImage(String userId, String imageId) throws IOException, ExecutionException, InterruptedException {

        if(albumId == null)
            checkAlbum();

        if(albumId == null){
            return Result.error(Result.ErrorCode.NOT_FOUND);
        }

        String request = GET_IMAGE_URL.replaceAll("\\{\\{imageId\\}\\}", imageId);
        OAuthRequest albumReq = new OAuthRequest(Verb.GET, request);
        albumReq.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
        service.signRequest(accessToken, albumReq);
        Response imageResp = service.execute(albumReq);

        if(imageResp.getCode() == HTTP_NOT_FOUND){
            return Result.error(Result.ErrorCode.NOT_FOUND);
        }else if(imageResp.getCode() != HTTP_SUCCESS)
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);

        BasicResponse response = json.fromJson(imageResp.getBody(), BasicResponse.class);
        String imageUrl = response.getData().get("link").toString();

        java.net.URL url = new java.net.URL(imageUrl);
        try (java.io.InputStream in = url.openStream();
             java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }

            return Result.ok(out.toByteArray());
        } catch (IOException e) {
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }


    @Override
    public Result<Void> deleteImage(String userId, String imageId, String password) throws IOException, ExecutionException, InterruptedException {

        Result<User> owner = getUsersClient().getUser(userId, password);

        if (!owner.isOK())
            return Result.error(owner.error());


        String delete = DELETE_IMAGE_URL.replaceAll("\\{\\{imageDeleteHash\\}\\}", imageId);
        OAuthRequest deleteReq = new OAuthRequest(Verb.DELETE, delete);

        deleteReq.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);

        service.signRequest(accessToken, deleteReq);
        Response deleteResp = service.execute(deleteReq);

        System.out.println("DELETE STATUS CODE: " + deleteResp.getCode());
        System.out.println("DELETE RESPONSE BODY: " + deleteResp.getBody());

        if(deleteResp.getCode() == HTTP_NOT_FOUND)
            return Result.error(Result.ErrorCode.NOT_FOUND);
        else {
            if (deleteResp.getCode() != HTTP_SUCCESS) {
                System.out.println("Arcadia90");
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            }
        }

        return Result.ok();

    }

    public static void setHostName(String hostName){
        albumName = hostName;
    }

    //checka se o album já tá no imgur
    private void checkAlbum() throws IOException, ExecutionException, InterruptedException {
        OAuthRequest listAlbumsRequest = new OAuthRequest(Verb.GET, "https://api.imgur.com/3/account/me/albums/");
        service.signRequest(accessToken, listAlbumsRequest);
        Response listAlbumsResponse = service.execute(listAlbumsRequest);

        if (listAlbumsResponse.getCode() == HTTP_SUCCESS) {
            AlbumListResponse albums = json.fromJson(listAlbumsResponse.getBody(), AlbumListResponse.class);
            for (Album album : albums.getData()) {
                if (albumName.equals(album.getTitle())) {
                    albumId = album.getId();
                    break;
                }
            }
        }
    }

    public void reboot() throws IOException, ExecutionException, InterruptedException {
        checkAlbum();
        if(albumId != null){
            String request = GET_ALBUM_IMAGES_URL.replaceAll("\\{\\{albumHash\\}\\}", albumId);
            OAuthRequest getAlbumImagesRequest = new OAuthRequest(Verb.GET, request);
            service.signRequest(accessToken, getAlbumImagesRequest);
            Response albumImagesResponse = service.execute(getAlbumImagesRequest);
            if(albumImagesResponse.getCode() == HTTP_SUCCESS) {
                AlbumImagesResponse imagesResponse = json.fromJson(albumImagesResponse.getBody(), AlbumImagesResponse.class);
                List<ImgurImageFromAlbum> images = imagesResponse.getData();

                if (images == null || images.isEmpty()) {
                    System.out.println("Sem imagens no álbum.");
                    return;
                }

                for (ImgurImageFromAlbum image : images) {
                    String deleteHash = image.getDeletehash();
                    if (deleteHash != null) {
                        String deleteUrl = DELETE_IMAGE_URL.replaceAll("\\{\\{imageDeleteHash\\}\\}", deleteHash);
                        OAuthRequest deleteReq = new OAuthRequest(Verb.DELETE, deleteUrl);
                        deleteReq.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
                        service.signRequest(accessToken, deleteReq);
                        Response deleteResp = service.execute(deleteReq);

                        System.out.println("Deleted image: " + image.getId() + " → Status: " + deleteResp.getCode());
                    }
                }
            }

        } else {
            System.out.println("Álbum não existe.");
        }
    }

}
