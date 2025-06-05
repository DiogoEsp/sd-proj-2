package fctreddit.impl.server.rest.replication;

import fctreddit.api.Post;
import fctreddit.api.PostVote;
import fctreddit.api.User;
import fctreddit.api.java.Content;
import fctreddit.api.java.Result;
import fctreddit.api.java.Users;
import fctreddit.api.rest.RestContent;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.java.JavaContent;
import fctreddit.impl.server.java.JavaContentRep;
import fctreddit.impl.server.java.JavaServer;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class PreCondicions extends JavaServer implements Content {
    private static Logger Log = Logger.getLogger(PreCondicions.class.getName());

    private Hibernate hibernate;
    private final Users usersClient;

    public PreCondicions(Hibernate hibernate, Users usersClient){
        this.hibernate = hibernate;
        this.usersClient = usersClient;
    }

    @Override
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

    @Override
    public Result<List<String>> getPosts(long timestamp, String sortOrder) {
        return null;
    }

    @Override
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

    @Override
    public Result<Void> deletePost(String postId, String userPassword) {
        return null;
    }

    @Override
    public Result<Void> upVotePost(String postId, String userId, String userPassword) {
        Log.info("Executing upVote on " + postId + " with Userid:" + userId + " Password: " + userPassword);

        if (userPassword == null)
            return Result.error(Result.ErrorCode.FORBIDDEN);

        Result<User> u = usersClient.getUser(userId, userPassword);
        if (!u.isOK())
            return Result.error(u.error());

        Log.info("Retrieved user: " + u.value());

        Hibernate.TX tx = hibernate.beginTransaction();

        Post p = hibernate.get(tx, Post.class, postId);

        if (p == null) {
            return Result.error(Result.ErrorCode.NOT_FOUND);
        }

        hibernate.abortTransaction(tx);

        return Result.ok();
    }


    @Override
    public Result<Void> removeUpVotePost(String postId, String userId, String userPassword) {
        Log.info("Executing removeUpVote on " + postId + " with Userid:" + userId + " Password: " + userPassword);

        if (userPassword == null)
            return Result.error(Result.ErrorCode.FORBIDDEN);

        Result<User> u = this.getUsersClient().getUser(userId, userPassword);
        if (!u.isOK())
            return Result.error(u.error());

        Log.info("Retrieved user: " + u.value());

        Hibernate.TX tx = hibernate.beginTransaction();

        List<PostVote> i = hibernate.sql(tx, "SELECT * from PostVote pv WHERE pv.userId='" + userId
                + "' AND pv.postId='" + postId + "' AND pv.upVote='true'", PostVote.class);
        if (i.size() == 0) {
            hibernate.abortTransaction(tx);
            return Result.error(Result.ErrorCode.NOT_FOUND);
        }

        hibernate.abortTransaction(tx);

        return Result.ok();
    }

    @Override
    public Result<Void> downVotePost(String postId, String userId, String userPassword) {
        Log.info("Executing downVote on " + postId + " with Userid:" + userId + " Password: " + userPassword);

        if (userPassword == null)
            return Result.error(Result.ErrorCode.FORBIDDEN);

        Result<User> u = usersClient.getUser(userId, userPassword);
        if (!u.isOK())
            return Result.error(u.error());

        Log.info("Retrieved user: " + u.value());

        Hibernate.TX tx = hibernate.beginTransaction();

        Post p = hibernate.get(tx, Post.class, postId);

        if (p == null) {
            hibernate.abortTransaction(tx);
            return Result.error(Result.ErrorCode.NOT_FOUND);
        }

        hibernate.abortTransaction(tx);

        return Result.ok();

    }

    @Override
    public Result<Void> removeDownVotePost(String postId, String userId, String userPassword) {
        return null;
    }

    @Override
    public Result<Integer> getupVotes(String postId) {
        Log.info("Executing getUpVotes on " + postId);
        Post p = hibernate.get(Post.class, postId);
        if (p == null)
            return Result.error(Result.ErrorCode.NOT_FOUND);

        List<Integer> count = hibernate.sql(
                "SELECT COUNT(*) from PostVote pv WHERE pv.postId='" + postId + "'  AND pv.upVote='true'",
                Integer.class);
        return Result.ok(count.iterator().next());

    }

    @Override
    public Result<Integer> getDownVotes(String postId) {
        Log.info("Executing getDownVotes on " + postId);
        Post p = hibernate.get(Post.class, postId);
        if (p == null)
            return Result.error(Result.ErrorCode.NOT_FOUND);

        List<Integer> count = hibernate.sql(
                "SELECT COUNT(*) from PostVote pv WHERE pv.postId='" + postId + "' AND pv.upVote='false'",
                Integer.class);
        return Result.ok(count.iterator().next());
    }

    @Override
    public Result<Void> removeTracesOfUser(String userId) {
        return null;
    }

    @Override
    public Result<Post> getPost(String postId) {
        Log.info("PRE Checking Null Post: ");

        Post p = hibernate.get(Post.class, postId);

        Result<Integer> res = this.getupVotes(postId);
        if (res.isOK())
            p.setUpVote(res.value());
        res = this.getDownVotes(postId);
        if (res.isOK())
            p.setDownVote(res.value());

        if (p != null)
            return Result.ok(p);
        else return Result.error(Result.ErrorCode.NOT_FOUND);
    }

    @Override
    public Result<List<String>> getPostAnswers(String postId, long maxTimeout) {
        long startOperation = System.currentTimeMillis();
        Log.info("Getting Answers for Post " + postId + " maxTimeout=" + maxTimeout);

        Post p = hibernate.get(Post.class, postId);
        if (p == null)
            return Result.error(Result.ErrorCode.NOT_FOUND);

        String parentURL = RestContent.PATH + "/" + postId;
        List<String> list = null;
        try {
            list = hibernate.sql(
                    "SELECT p.postId from Post p WHERE p.parentURL LIKE '" + parentURL + "' ORDER BY p.creationTimestamp",
                    String.class);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        if (maxTimeout > 0) {
            String lock = null;
            synchronized (JavaContentRep.postLocks) {
                lock = JavaContentRep.postLocks.get(postId);
            }
            synchronized (lock) {
                long deadline = startOperation + maxTimeout;

                while (System.currentTimeMillis() < deadline) {

                    try {
                        long waitTime = deadline - System.currentTimeMillis();
                        if(waitTime > 0)
                            lock.wait(waitTime);
                    } catch (InterruptedException e) {
                        // Ignore this case...
                    }

                    List<String> redo = null;
                    try {
                        redo = hibernate.sql("SELECT p.postId from Post p WHERE p.parentURL='" + parentURL
                                + "' ORDER BY p.creationTimestamp", String.class);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
                    }

                    if (redo.size() > list.size()) {
                        list = redo;
                        break;
                    }

                }
            }
        }

        return Result.ok(list);
    }
}
