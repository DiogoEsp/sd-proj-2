package fctreddit.impl.server.rest.replication;

import fctreddit.api.Post;
import fctreddit.api.PostVote;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestContent;
import fctreddit.impl.kafka.KafkaPublisher;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.java.JavaContentRep;
import fctreddit.impl.server.java.JavaServer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class ContentEffects extends JavaServer {

    private final Hibernate hibernate = Hibernate.getInstance();
    private final Logger Log = Logger.getLogger(ContentEffects.class.getName());
    private final KafkaPublisher publisher;
    private static HashMap<String, String> postLocks;
    private final String serverURI;


    public ContentEffects(KafkaPublisher publisher, HashMap<String, String> postLocks, String serverUri) {
        this.publisher = publisher;
        this.postLocks = postLocks;
        this.serverURI = serverUri;
    }

    public Result<String> createPost(Post post) {
        Log.info("tá no effects");
        Hibernate.TX tx = hibernate.beginTransaction();

        if (post.getParentUrl() != null && !post.getParentUrl().isBlank()) {
            String postID = extractResourceID(post.getParentUrl());
            Log.info("Trying to check if parent post exists: " + postID);
            Post p = hibernate.get(tx, Post.class, postID);
            if (p == null) {
                hibernate.abortTransaction(tx);
                return Result.error(Result.ErrorCode.NOT_FOUND);
            }
        }

        post.setUpVote(0);
        post.setDownVote(0);

        Log.info("Trying to store post");

        while (true) {
            try {
                hibernate.persist(tx, post);
                hibernate.commitTransaction(tx);
            } catch (Exception ex) { // The transaction has failed, which means we have to restart the whole
                // transaction
                Log.info("Failed to commit transaction creating post");
                ex.printStackTrace();

                hibernate.abortTransaction(tx);
                Log.info("Aborting and restarting...");
                tx = hibernate.beginTransaction();
                if (post.getParentUrl() != null && !post.getParentUrl().isBlank()) {
                    String postID = extractResourceID(post.getParentUrl());
                    Log.info("Trying to check if parent post exists: " + postID);
                    Post p = hibernate.get(tx, Post.class, postID);
                    if (p == null) {
                        hibernate.abortTransaction(tx);
                        return Result.error(Result.ErrorCode.NOT_FOUND);
                    }
                }
                continue;
            }
            break;
        }

        if (post.getMediaUrl() != null && !post.getMediaUrl().isBlank()) {
            String stringBuilt = "create " + post.getMediaUrl();
            publisher.publish("posts", stringBuilt);
        }

        try {
            // To unlock waiting threads
            synchronized (JavaContentRep.postLocks) {
                JavaContentRep.postLocks.put(post.getPostId(), post.getPostId());

                if (post.getParentUrl() != null) {
                    String parentId = extractResourceID(post.getParentUrl());
                    String lock = JavaContentRep.postLocks.get(parentId);
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        } catch (Exception e) {
            // Unable to notify due tome strange event.
            Log.info("Ubale to notify potentiallly waiting threads due to: " + e.getMessage());
            e.printStackTrace();
        }


        return Result.ok(post.getPostId());
    }

    public Result<Post> updatepost(Post p, Post post) {

        Hibernate.TX tx = hibernate.beginTransaction();

        // We can update finally
        if (post.getContent() != null)
            p.setContent(post.getContent());
        if (post.getMediaUrl() != null) {
            String stringBuilt = "delete " + p.getMediaUrl();
            publisher.publish("posts", stringBuilt);
            p.setMediaUrl(post.getMediaUrl());
            stringBuilt = "create " + p.getMediaUrl();
            publisher.publish("posts", stringBuilt);
        }


        try {
            hibernate.persist(tx, p);
            hibernate.commitTransaction(tx);
        } catch (Exception e) {
            hibernate.abortTransaction(tx);
            return Result.error(Result.ErrorCode.BAD_REQUEST);
        }

        return Result.ok(p);
    }

    public Result<Post> deletePost( Post p){

        Hibernate.TX tx = hibernate.beginTransaction();

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
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        return Result.ok();
    }

    public Result<Void> upVote(String userId, String postId){

        Hibernate.TX tx = hibernate.beginTransaction();
        try {
            hibernate.persist(tx, new PostVote(userId, postId, true));
            hibernate.commitTransaction(tx);
        } catch (Exception e) {
            hibernate.abortTransaction(tx);
            return Result.error(Result.ErrorCode.CONFLICT);
        }

        hibernate.abortTransaction(tx);

        return Result.ok();
    }

    public Result<Void> downVote(String userId, String postId){
        Hibernate.TX tx = hibernate.beginTransaction();
        try {
            hibernate.persist(tx, new PostVote(userId, postId, false));
            hibernate.commitTransaction(tx);
        } catch (Exception e) {
            hibernate.abortTransaction(tx);
            return Result.error(Result.ErrorCode.CONFLICT);
        }

        hibernate.abortTransaction(tx);

        return Result.ok();

    }

    public Result<Void> remDownVote(List<PostVote> i){
        Hibernate.TX tx = hibernate.beginTransaction();
        try {
            hibernate.delete(tx, i.iterator().next());
            hibernate.commitTransaction(tx);
        } catch (Exception e) {
            hibernate.abortTransaction(tx);
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

        return Result.ok();
    }




}
