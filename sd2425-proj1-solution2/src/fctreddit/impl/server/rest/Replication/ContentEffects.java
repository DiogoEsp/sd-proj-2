package fctreddit.impl.server.rest.Replication;

import fctreddit.api.Post;
import fctreddit.api.java.Content;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestContentRep;
import fctreddit.impl.kafka.KafkaPublisher;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.java.JavaContentRep;
import fctreddit.impl.server.java.JavaServer;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class ContentEffects extends JavaServer {

    private Hibernate hibernate;
    private Logger Log;
    private KafkaPublisher publisher;
    private HashMap<String, String> postLocks;
    private String serverURI;


    public ContentEffects(Hibernate hibermnate, Logger Log, KafkaPublisher publisher, HashMap<String, String> postLocks, String serverUri){
        this.hibernate = hibermnate;
        this.Log = Log;
        this.publisher = publisher;
        this.postLocks = postLocks;
        this.serverURI = serverUri;
    }

    public Result<String> createPost(Post post){

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

        post.setCreationTimestamp(System.currentTimeMillis());
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



}
