package fctreddit.impl.server.java;

import java.util.*;
import java.util.logging.Logger;

import com.google.gson.Gson;
import fctreddit.api.Post;
import fctreddit.api.PostVote;
import fctreddit.api.User;
import fctreddit.api.java.Content;
import fctreddit.api.java.Result;
import fctreddit.api.java.Result.ErrorCode;
import fctreddit.api.rest.RestContent;
import fctreddit.api.rest.RestContentRep;
import fctreddit.impl.kafka.KafkaPublisher;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.Hibernate.TX;
import fctreddit.impl.server.rest.replication.ContentEffects;
import fctreddit.impl.server.rest.replication.PreCondicions;
import fctreddit.impl.server.rest.filter.VersionFilter;
import fctreddit.utils.GetPostArg;
import fctreddit.utils.SyncPoint;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class JavaContentRep extends JavaServer implements Content {

    public static JavaContentRep instance;
    private static final String REP_TOPIC = "replication";

    private static final String CREATE = "CREATE";
    private static final String UPDATE = "UPDATE";
    private static final String GET = "GET";
    private static final String UP_VOTE = "UPVOTE";
    private static final String DOWN_VOTE = "DOWNVOTE";
    private static final String RDOWN_VOTE = "RDOWNVOTE";
    private static final String RUP_VOTE = "RUPVOTE";

    private static Logger Log = Logger.getLogger(JavaContentRep.class.getName());
    private static final Gson gson = new Gson();
    private Hibernate hibernate;

    public static final HashMap<String, String> postLocks = new HashMap<String, String>();

    public static String serverURI;
    private PreCondicions pc;
    private ContentEffects ce;
    private static KafkaPublisher publisher;
    private SyncPoint syncPoint;
    private static KafkaPublisher repPublisher;

    public record ReplicationMessage(
            String operation,
            String jsonArgs
    ) {
    }

    public static JavaContentRep getInstance() {
        if (instance == null)
            instance = new JavaContentRep();
        return instance;
    }

    private JavaContentRep() {
        hibernate = Hibernate.getInstance();
        this.pc = new PreCondicions(hibernate, getUsersClient());
        this.ce = new ContentEffects(publisher, postLocks, serverURI);
        this.syncPoint = SyncPoint.getSyncPoint();
    }

    public static void setKafka(KafkaPublisher publisher) {
        if (JavaContentRep.publisher == null)
            JavaContentRep.publisher = publisher;
    }

    public static void setKafkaRep(KafkaPublisher publisherRep) {
        if (JavaContentRep.repPublisher == null)
            JavaContentRep.repPublisher = publisherRep;
    }

    public static void setServerURI(String serverURI) {
        if (JavaContentRep.serverURI == null)
            JavaContentRep.serverURI = serverURI;
    }

    public void handleDeletedImages(String value) {
        Log.info("Received image deletion message: " + value);
        String imageUrl = value.trim();
        if (imageUrl.isEmpty()) {
            Log.warning("Empty image ID received for deletion.");
            return;
        }
        try {
            TX tx = Hibernate.getInstance().beginTransaction();
            int deleted = Hibernate.getInstance().sql(tx, "UPDATE Post p SET p.mediaUrl=NULL where p.mediaUrl='" + imageUrl + "'");
            Hibernate.getInstance().commitTransaction(tx);
            Log.info("Deleted image with ID: " + imageUrl + ", affected rows: " + deleted);
        } catch (Exception e) {
            Log.severe("Failed to delete image with ID: " + imageUrl + " due to: " + e.getMessage());
        }
    }

    public void handleReplication(ConsumerRecord<String, String> record) {
        long offset = record.offset();
        String json = record.value();

        try {
            switch (record.key()) {
                case CREATE -> {
                    Post msg = gson.fromJson(json, Post.class);
                    Log.info("O meu post ta  aqi " + msg.toString());
                    Result<String> result = ce.createPost(msg);
                    Log.info("rArcadia" + result);
                    try {
                        syncPoint.setResult(offset, result);
                    } catch (Exception e) {
                        Log.severe(e.getMessage());
                    }
                }
                case UPDATE -> {
                    String[] args = json.split("///");
                    Post p = gson.fromJson(args[0], Post.class);
                    Post post = gson.fromJson(args[1], Post.class);
                    Result<Post> res = ce.updatePost(p, post);
                    try {
                        syncPoint.setResult(offset, res);
                    } catch (Exception e) {
                        Log.severe(e.getMessage());
                    }
                }
                case UP_VOTE -> {
                    String[] args = json.split("///");
                    Result<Void> res = ce.upVote(args[0], args[1]);
                    syncPoint.setResult(offset, res);
                }
                case DOWN_VOTE -> { String[] args = json.split("///");
                    Result<Void> res = ce.downVote(args[0], args[1]);
                    syncPoint.setResult(offset, res);}
                case RDOWN_VOTE -> {}
                case RUP_VOTE -> {}
                default -> System.out.println("Unknown operation");
            }
        } catch (Exception e) {
            Log.severe(e.getMessage());
        }
    }

    @Override
    public Result<String> createPost(Post post, String userPassword) {
        Result<String> pre = pc.createPost(post, userPassword);

        if (!pre.isOK()) {
            return pre;
        }
        String id = UUID.randomUUID().toString();
        post.setPostId(id);
        post.setCreationTimestamp(System.currentTimeMillis());


        String message = gson.toJson(post);

        long offset = publisher.publish(REP_TOPIC, CREATE, message);

        Result<?> r = syncPoint.waitForResult(offset);
        Log.info("O resultado do syncPoint" + r.toString());

        if (r.isOK()) return Result.ok((String) r.value());
        else {
            Log.info("Erro no syncPoint!? " + r.error());
            return Result.error(r.error());
        }
    }

    @Override
    public Result<List<String>> getPosts(long timestamp, String sortOrder) {

        Log.info("Getting Posts with timestamp=" + timestamp + " sortOrder=" + sortOrder);

        String baseSQLStatement = null;

        if (sortOrder != null && !sortOrder.isBlank()) {
            if (sortOrder.equalsIgnoreCase(Content.MOST_UP_VOTES)) {
                baseSQLStatement = "SELECT postId FROM (SELECT p.postId as postId, "
                        + "(SELECT COUNT(*) FROM PostVote pv where p.postId = pv.postId AND pv.upVote='true') as upVotes "
                        + "from Post p WHERE "
                        + (timestamp > 0 ? "p.creationTimestamp >= '" + timestamp + "' AND " : "")
                        + "p.parentURL IS NULL) ORDER BY upVotes DESC, postID ASC";
            } else if (sortOrder.equalsIgnoreCase(Content.MOST_REPLIES)) {
                baseSQLStatement = "SELECT postId FROM (SELECT p.postId as postId, "
                        + "(SELECT COUNT(*) FROM Post p2 where p2.parentUrl LIKE CONCAT('"
                        + RestContent.PATH + "/',p.postId)) as replies " + "from Post p WHERE "
                        + (timestamp > 0 ? "p.creationTimestamp >= '" + timestamp + "' AND " : "")
                        + "p.parentURL IS NULL) ORDER BY replies DESC, postID ASC";
            } else {
                Log.info("Invalid sortOrder: '" + sortOrder + "' going for default ordering...");
                baseSQLStatement = "SELECT p.postId from Post p WHERE "
                        + (timestamp > 0 ? "p.creationTimestamp >= '" + timestamp + "' AND " : "")
                        + "p.parentURL IS NULL ORDER BY p.creationTimestamp ASC";
            }
        } else {
            baseSQLStatement = "SELECT p.postId from Post p WHERE "
                    + (timestamp > 0 ? "p.creationTimestamp >= '" + timestamp + "' AND " : "")
                    + "p.parentURL IS NULL ORDER BY p.creationTimestamp ASC";
        }

        try {
            List<String> list = null;
            Log.info("Executing selection of Posts with the following query:\n" + baseSQLStatement);
            list = hibernate.sql(baseSQLStatement, String.class);
            /** Log.info("Output generated (in this order):");
             for (int i = 0; i < list.size(); i++) {
             Log.info("\t" + list.get(i).toString() + " \ttimestamp: "
             + hibernate.get(Post.class, list.get(i)).getCreationTimestamp() + " \tReplies: "
             + this.getPostAnswers(list.get(i), 0).value().size() + " \tUpvotes: "
             + this.getupVotes(list.get(i)).value());
             }*/
            return Result.ok(list);
        } catch (Exception e) {
            e.printStackTrace();
            throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public Result<Post> getPost(String postId) {
        return pc.getPost(postId);
    }

    @Override
    public Result<List<String>> getPostAnswers(String postId, long maxTimeout) {
        syncPoint.waitForVersion(VersionFilter.version.get());
        return pc.getPostAnswers(postId, maxTimeout);
    }

    @Override
    public Result<Post> updatePost(String postId, String userPassword, Post post) {

        Result<Post> res = pc.updatePost(postId, userPassword, post);
        Post p;
        if (!res.isOK()) {
            return res;
        } else {
            p = res.value();
        }

        String message = gson.toJson(p) + "///" + gson.toJson(post);

        long offset = publisher.publish(REP_TOPIC, UPDATE, message);
        Result<?> r = syncPoint.waitForResult(offset);


        if (r.isOK()) return Result.ok((Post) r.value());
        else {
            Log.info("Erro no syncPoint!? " + r.error());
            return Result.error(r.error());
        }
    }

    @Override
    public Result<Void> deletePost(String postId, String userPassword) {
        TX tx = hibernate.beginTransaction();

        Post p = hibernate.get(tx, Post.class, postId);

        if (p == null) {
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.NOT_FOUND);
        }

        if (p.getAuthorId() == null || userPassword == null) {
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.FORBIDDEN);
        }

        Result<User> u = this.getUsersClient().getUser(p.getAuthorId(), userPassword);
        if (!u.isOK())
            return Result.error(u.error());

        // We can delete... maybe get the entirety of descendants and start from back to
        // start.
        LinkedList<Post> pending = new LinkedList<Post>();
        pending.add(p);
        LinkedList<Post> allElementsToDelete = new LinkedList<Post>();

        while (!pending.isEmpty()) {
            Post current = pending.removeFirst();
            String parentURL = serverURI + RestContent.PATH + "/" + current.getPostId();
            List<String> descendants = hibernate.sql(tx,
                    "SELECT p.postId from Post p WHERE p.parentURL='" + parentURL + "' ORDER BY p.creationTimestamp",
                    String.class);
            for (String id : descendants) {
                Log.info("Fetching descendant post with ID: " + id);
                Post child = hibernate.get(tx, Post.class, id);
                if (child == null) {
                    Log.warning("Child post with ID " + id + " not found!");
                    continue;
                }
                pending.addLast(child);
            }

            allElementsToDelete.addFirst(current);
        }

        try {
            for (Post d : allElementsToDelete) {
                int number = hibernate.sql(tx, "DELETE from PostVote pv WHERE pv.postId='" + d.getPostId() + "'");
                Log.info("Deleted " + number + " votes (upVotes + downVotes)");
                hibernate.delete(tx, d);
                synchronized (JavaContentRep.postLocks) {
                    String s = JavaContentRep.postLocks.remove(d.getPostId());
                    if (s != null) {
                        synchronized (s) {
                            s.notifyAll();
                        }
                    }
                }

                if (d.getMediaUrl() != null) {
                    String imageId = extractResourceID(d.getMediaUrl());
                    String stringBuilt = "delete " + d.getMediaUrl();
                    publisher.publish("posts", stringBuilt);

                }
            }
            if (p.getMediaUrl() != null)
                hibernate.commitTransaction(tx);

        } catch (Exception e) {
            e.printStackTrace();
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.INTERNAL_ERROR);
        }

        return Result.ok();
    }

    @Override
    public Result<Void> upVotePost(String postId, String userId, String userPassword) {
        Result<Void> res = pc.upVotePost(postId, userId, userPassword);

        if(!res.isOK()){
            return res;
        }

        String message = userId + "///" + postId;
        long offset = publisher.publish(REP_TOPIC, UP_VOTE, message);
        Result<?> r = syncPoint.waitForResult(offset);

        if (r.isOK()) return Result.ok();
        else {
            Log.info("Erro no syncPoint!? " + r.error());
            return Result.error(r.error());
        }
    }

    @Override
    public Result<Void> removeUpVotePost(String postId, String userId, String userPassword) {
        Log.info("Executing removeUpVote on " + postId + " with Userid:" + userId + " Password: " + userPassword);

        if (userPassword == null)
            return Result.error(ErrorCode.FORBIDDEN);

        Result<User> u = this.getUsersClient().getUser(userId, userPassword);
        if (!u.isOK())
            return Result.error(u.error());

        Log.info("Retrieved user: " + u.value());

        TX tx = hibernate.beginTransaction();

        List<PostVote> i = hibernate.sql(tx, "SELECT * from PostVote pv WHERE pv.userId='" + userId
                + "' AND pv.postId='" + postId + "' AND pv.upVote='true'", PostVote.class);
        if (i.size() == 0) {
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.NOT_FOUND);
        }

        try {
            hibernate.delete(tx, i.iterator().next());
            hibernate.commitTransaction(tx);
        } catch (Exception e) {
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.INTERNAL_ERROR);
        }

        return Result.ok();
    }

    @Override
    public Result<Void> downVotePost(String postId, String userId, String userPassword) {
        Result<Void> res = pc.downVotePost(postId, userId, userPassword);

        if(!res.isOK()){
            return res;
        }

        String message = userId + "///" + postId;
        long offset = publisher.publish(REP_TOPIC, DOWN_VOTE, message);
        Result<?> r = syncPoint.waitForResult(offset);

        if (r.isOK()) return Result.ok();
        else {
            Log.info("Erro no syncPoint!? " + r.error());
            return Result.error(r.error());
        }
    }

    @Override
    public Result<Void> removeDownVotePost(String postId, String userId, String userPassword) {
        Log.info("Executing removeDownVote on " + postId + " with Userid:" + userId + " Password: " + userPassword);

        if (userPassword == null)
            return Result.error(ErrorCode.FORBIDDEN);

        Result<User> u = this.getUsersClient().getUser(userId, userPassword);
        if (!u.isOK())
            return Result.error(u.error());

        Log.info("Retrieved user: " + u.value());

        TX tx = hibernate.beginTransaction();

        List<PostVote> i = hibernate.sql(tx, "SELECT * from PostVote pv WHERE pv.userId='" + userId
                + "' AND pv.postId='" + postId + "' AND pv.upVote='false'", PostVote.class);
        if (i.size() == 0) {
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.NOT_FOUND);
        }

        try {
            hibernate.delete(tx, i.iterator().next());
            hibernate.commitTransaction(tx);
        } catch (Exception e) {
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.INTERNAL_ERROR);
        }

        return Result.ok();
    }

    @Override
    public Result<Integer> getupVotes(String postId) {
        return pc.getupVotes(postId);
    }

    @Override
    public Result<Integer> getDownVotes(String postId) {
        return pc.getDownVotes(postId);
    }

    @Override
    public Result<Void> removeTracesOfUser(String userId) {
        Log.info("Executing a removeTracesOfUser on " + userId);
        TX tx = null;
        try {
            tx = hibernate.beginTransaction();

            hibernate.sql(tx, "DELETE from PostVote pv where pv.userId='" + userId + "'");

            hibernate.sql(tx, "UPDATE Post p SET p.authorId=NULL where p.authorId='" + userId + "'");

            hibernate.commitTransaction(tx);

        } catch (Exception e) {
            e.printStackTrace();
            hibernate.abortTransaction(tx);
            return Result.error(ErrorCode.INTERNAL_ERROR);
        }

        return Result.ok();
    }

}
