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
    private static Logger Log = Logger.getLogger(PreCondicions.class.getName());

    private Hibernate hibernate;

    public PreCondicions(Hibernate hibernate){
        this.hibernate = hibernate;
    }

    public Result<String> createPost(Post post, String userPassword){
        Log.info("Checking Pre Conditions for CreatePost");
        if (post.getAuthorId() == null || post.getAuthorId().isBlank() || post.getContent() == null
                || post.getContent().isBlank())
            return Result.error(Result.ErrorCode.BAD_REQUEST);

        if (userPassword == null)
            return Result.error(Result.ErrorCode.FORBIDDEN);
        Users uc = getUsersClient();

        Result<User> owner = uc.getUser(post.getAuthorId(), userPassword);
        if (!owner.isOK()) {
            Log.info("Erro no owner WHAT?!?!?: " + owner.error());
            return Result.error(owner.error());
        }

        return Result.ok();
    }

    public Result<Post> getPost(Post p) {
        Log.info("PRE Checking Null Post: " + (p == null));

        if (p != null)
            return Result.ok();
        else return Result.error(Result.ErrorCode.NOT_FOUND);
    }

}
