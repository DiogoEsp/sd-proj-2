package fctreddit.impl.server.rest.replication;

import fctreddit.api.Post;
import fctreddit.api.java.Content;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestContent;
import fctreddit.impl.server.java.JavaContentRep;
import fctreddit.impl.server.rest.RestResource;
import fctreddit.impl.server.rest.filter.VersionFilter;
import fctreddit.utils.SyncPoint;
import jakarta.ws.rs.WebApplicationException;

import java.util.List;
import java.util.logging.Logger;

public class ContentRepResource extends RestResource implements RestContent {

    private Content impl;
    private Logger Log= Logger.getLogger(ContentRepResource.class.getName());
    private SyncPoint syncPoint;

    public ContentRepResource() {
        this.impl = JavaContentRep.getInstance();
        this.syncPoint = SyncPoint.getSyncPoint();
    }

    @Override
    public String createPost(Post post, String userPassword) {
        Result<String> res = impl.createPost(post, userPassword);

        Log.info("resource" + res.toString());

        if(res.isOK()) {
            Log.info("foi ok");
            return res.value();
        }
        else
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public List<String> getPosts(long timestamp, String sortOrder) {
        Long version = VersionFilter.version.get();
        syncPoint.waitForVersion(version);

        Result<List<String>> res = impl.getPosts(timestamp, sortOrder);

        if(res.isOK())
            return res.value();
        else
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public Post getPost(String postId) {
        Long version = VersionFilter.version.get();

        syncPoint.waitForVersion(version);
        Result<Post> res = impl.getPost(postId);

        if(res.isOK())
            return res.value();
        else
            throw new WebApplicationException(errorCodeToStatus(res.error()));
    }

    @Override
    public List<String> getPostAnswers(String postId, long timeout) {
        Long version = VersionFilter.version.get();
        syncPoint.waitForVersion(version);
        Result<List<String>> res = impl.getPostAnswers(postId, timeout);

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
        Long version = VersionFilter.version.get();
        syncPoint.waitForVersion(version);

        Result<Integer> res = impl.getupVotes(postId);

        if(!res.isOK())
            throw new WebApplicationException(errorCodeToStatus(res.error()));

        return res.value();
    }

    @Override
    public Integer getDownVotes(String postId) {
        Long version = VersionFilter.version.get();
        syncPoint.waitForVersion(version);

        Result<Integer> res = impl.getDownVotes(postId);

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
