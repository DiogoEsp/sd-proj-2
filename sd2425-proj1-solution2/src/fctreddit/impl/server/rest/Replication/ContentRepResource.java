package fctreddit.impl.server.rest.Replication;

import fctreddit.api.Post;
import fctreddit.api.java.Content;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestContent;
import fctreddit.impl.server.java.JavaContent;
import fctreddit.impl.server.rest.RestResource;
import fctreddit.impl.server.rest.filter.VersionFilter;
import jakarta.ws.rs.WebApplicationException;

import java.util.List;

public class ContentRepResource extends RestResource implements RestContent {
    private Content impl;

    public ContentRepResource() {
        this.impl = new JavaContent();
    }

    @Override
    public String createPost(Post post, String userPassword) {
        Result<String> res = impl.createPost(post, userPassword);

        if(res.isOK())
            return res.value();
        else
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public List<String> getPosts(long timestamp, String sortOrder) {
        Result<List<String>> res = impl.getPosts(timestamp, sortOrder);

        Long clientVersion = VersionFilter.version.get();

        if(res.isOK())
            return res.value();
        else
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public Post getPost(String postId) {
        Result<Post> res = impl.getPost(postId);

        Long clientVersion = VersionFilter.version.get();

        if(res.isOK())
            return res.value();
        else
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public List<String> getPostAnswers(String postId, long timeout) {
        Result<List<String>> res = impl.getPostAnswers(postId, timeout);

        Long clientVersion = VersionFilter.version.get();

        if(res.isOK()) {
            return res.value();
        } else {
            throw new WebApplicationException(errorCodeToStatus(res.error()));
        }
    }

    @Override
    public Post updatePost(String postId, String userPassword, Post post) {
        Result<Post> res = impl.updatePost(postId, userPassword, post);

        Long clientVersion = VersionFilter.version.get();

        if(res.isOK())
            return res.value();
        else
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public void deletePost(String postId, String userPassword) {
        Result<Void> res = impl.deletePost(postId, userPassword);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public void upVotePost(String postId, String userId, String userPassword) {
        Result<Void> res = impl.upVotePost(postId, userId, userPassword);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public void removeUpVotePost(String postId, String userId, String userPassword) {
        Result<Void> res = impl.removeUpVotePost(postId, userId, userPassword);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public void downVotePost(String postId, String userId, String userPassword) {
        Result<Void> res = impl.downVotePost(postId, userId, userPassword);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public void removeDownVotePost(String postId, String userId, String userPassword) {
        Result<Void> res = impl.removeDownVotePost(postId, userId, userPassword);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public Integer getupVotes(String postId) {
        Result<Integer> res = impl.getupVotes(postId);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));

        return res.value();
    }

    @Override
    public Integer getDownVotes(String postId) {
        Result<Integer> res = impl.getDownVotes(postId);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));

        return res.value();
    }

    @Override
    public void removeTracesOfUser(String userId) {
        Result<Void> res = impl.removeTracesOfUser(userId);

        Long clientVersion = VersionFilter.version.get();

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));

    }


}
