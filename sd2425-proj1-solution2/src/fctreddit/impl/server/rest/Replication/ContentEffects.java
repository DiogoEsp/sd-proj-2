package fctreddit.impl.server.rest.Replication;

import fctreddit.api.Post;
import fctreddit.api.java.Result;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.java.JavaContentRep;
import fctreddit.impl.server.java.JavaServer;

import java.util.UUID;
import java.util.logging.Logger;

public class ContentEffects extends JavaServer {

    private Hibernate hibernate;
    private Logger Log;


    public ContentEffects(Hibernate hibermnate, Logger Log){
        this.hibernate = hibermnate;
        this.Log = Log;

    }

    public Result<String> createPost(Post post){

       return Result.ok();
    }

}
