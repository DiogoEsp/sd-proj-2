package fctreddit.utils;

import fctreddit.api.Post;

public class CreatePostReplicationData {
    private Post post;
    private String userPassword;

    // Default constructor (needed for Gson)
    public CreatePostReplicationData() {}

    public CreatePostReplicationData(Post post, String userPassword) {
        this.post = post;
        this.userPassword = userPassword;
    }

    public Post getPost() {
        return post;
    }

    public String getUserPassword() {
        return userPassword;
    }
}