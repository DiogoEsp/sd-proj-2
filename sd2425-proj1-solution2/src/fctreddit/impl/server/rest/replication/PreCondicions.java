package fctreddit.impl.server.rest.replication;

import fctreddit.api.Post;
import fctreddit.api.User;
import fctreddit.api.java.Result;
import fctreddit.api.java.Users;
import fctreddit.api.rest.RestContent;
import fctreddit.impl.server.Hibernate;

import java.util.List;
import java.util.logging.Logger;

public class PreCondicions  {
    private static Logger Log = Logger.getLogger(PreCondicions.class.getName());

    private Hibernate hibernate;
    private final Users usersClient;

    public PreCondicions(Hibernate hibernate, Users usersClient){
        this.hibernate = hibernate;
        this.usersClient = usersClient;
    }

    public Result<String> createPost(Post post, String userPassword){
        Log.info("Checking Pre Conditions for CreatePost");
        if (post.getAuthorId() == null || post.getAuthorId().isBlank() || post.getContent() == null
                || post.getContent().isBlank())
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        if (userPassword == null)
            return Result.error(Result.ErrorCode.FORBIDDEN);
        //Users uc = getUsersClient();

        Result<User> owner = usersClient.getUser(post.getAuthorId(), userPassword);
        if (!owner.isOK()) {
            Log.info("Erro no owner WHAT?!?!?: " + owner.error());
            return Result.error(owner.error());
        }

        return Result.ok();
    }

    public Result<Post> updatePost(String postId, String userPassword, Post post){
        Hibernate.TX tx = hibernate.beginTransaction();

        Post p = hibernate.get(tx, Post.class, postId);

        if (p == null) {
            hibernate.abortTransaction(tx);
            return Result.error(Result.ErrorCode.NOT_FOUND);
        }

        if (post.getPostId() != null) {
            hibernate.abortTransaction(tx);
            Log.info("Cannot update post" + postId + ", since the postId cannot be updated");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        if (post.getAuthorId() != null) {
            hibernate.abortTransaction(tx);
            Log.info("Cannot update post" + postId + ", since the authordId cannot be updated");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        if (userPassword == null) {
            hibernate.abortTransaction(tx);
            Log.info("Cannot update post" + postId + ", since no user password was provided");
            return Result.error(Result.ErrorCode.FORBIDDEN);
        }


        Result<User> u = usersClient.getUser(post.getAuthorId(), userPassword);
        if (!u.isOK()) {
            hibernate.abortTransaction(tx);
            return Result.error(u.error());
        }

        // Check if there are answers
        String parentURL = RestContent.PATH + "/" + postId;
        if (!hibernate.sql(tx, "SELECT p.postId from Post p WHERE p.parentURL LIKE '" + parentURL + "'", String.class).isEmpty()) {
            hibernate.abortTransaction(tx);
            Log.info("Cannot update post" + postId + ", since there is at least one answer.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        // Check if there are votes
        List<Integer> resp = hibernate.sql(tx, "SELECT COUNT(*) from PostVote pv WHERE pv.postId='" + postId + "'",
                Integer.class);
        if (resp.iterator().next() > 0) {
            hibernate.abortTransaction(tx);
            Log.info("Cannot update post" + postId + ", since there is at least one upVote.");
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        return Result.ok(p);
    }

    public Result<Post> getPost(Post p) {
        Log.info("PRE Checking Null Post: " + (p == null));

        if (p != null)
            return Result.ok();
        else return Result.error(Result.ErrorCode.NOT_FOUND);
    }

}
