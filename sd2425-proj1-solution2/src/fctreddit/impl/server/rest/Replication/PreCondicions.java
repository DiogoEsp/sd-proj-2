package fctreddit.impl.server.rest.Replication;

import fctreddit.api.Post;
import fctreddit.api.User;
import fctreddit.api.java.Result;
import fctreddit.api.java.Users;
import fctreddit.impl.client.UsersClient;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.java.JavaContentRep;
import org.hibernate.event.spi.ResolveNaturalIdEventListener;

import java.util.logging.Logger;

public class PreCondicions extends JavaContentRep {

    private Hibernate hibernate;
    private Logger Log;

    public PreCondicions(Hibernate hibernate, Logger Log){
        this.hibernate = hibernate;
        this.Log = Log;
    }
    public Result<String> createUser(Post post, String userPassword){
        if (post.getAuthorId() == null || post.getAuthorId().isBlank() || post.getContent() == null
                || post.getContent().isBlank())
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        if (userPassword == null)
            return Result.error(Result.ErrorCode.FORBIDDEN);
        Users uc = getUsersClient();

        Result<User> owner = uc.getUser(post.getAuthorId(), userPassword);
        if (!owner.isOK()) {
            return Result.error(owner.error());
        }

        return Result.ok();
    }

    public Result<String> updatePostPre(String postId, String userPassword, Post post) {
        return Result.ok();
    }
}
