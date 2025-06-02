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
import fctreddit.api.java.Users;
import fctreddit.api.rest.RestContent;
import fctreddit.impl.client.UsersClient;
import fctreddit.impl.kafka.KafkaPublisher;
import fctreddit.impl.server.Hibernate;
import fctreddit.impl.server.Hibernate.TX;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class JavaContent extends JavaServer implements Content {

	private static Logger Log = Logger.getLogger(JavaContent.class.getName());
	private static final Gson gson = new Gson();
	private Hibernate hibernate;

	private static final HashMap<String, String> postLocks = new HashMap<String, String>();

	private static String serverURI;
	private static KafkaPublisher publisher;

	public JavaContent() {
		hibernate = Hibernate.getInstance();

	}

	public static void setKafka(KafkaPublisher publisher) {
		if (JavaContent.publisher == null)
			JavaContent.publisher = publisher;
	}

	public static void setServerURI(String serverURI) {
		if (JavaContent.serverURI == null)
			JavaContent.serverURI = serverURI;
	}

	public static void handleDeletedImages(String value) {
		Log.info("Received image deletion message: " + value);
		// Supondo que o value seja o ID da imagem a eliminar
		String imageUrl = value.trim();
		if (imageUrl.isEmpty()) {
			Log.warning("Empty image ID received for deletion.");
			return;
		}
		try {
			// Aqui, implementa o código para remover referências ou dados relacionados a essa imagem
			// Exemplo simples: apagar da base de dados (pseudocódigo)
			TX tx = Hibernate.getInstance().beginTransaction();
			int deleted = Hibernate.getInstance().sql(tx, "UPDATE Post p SET p.mediaUrl=NULL where p.mediaUrl='" + imageUrl + "'");
			Hibernate.getInstance().commitTransaction(tx);
			Log.info("Deleted image with ID: " + imageUrl + ", affected rows: " + deleted);
		} catch (Exception e) {
			Log.severe("Failed to delete image with ID: " + imageUrl + " due to: " + e.getMessage());
		}
	}

	@Override
	public Result<String> createPost(Post post, String userPassword) {
		Log.info("Registering new post: " + post.toString() + " with password: " + userPassword);

		if (post.getAuthorId() == null || post.getAuthorId().isBlank() || post.getContent() == null
				|| post.getContent().isBlank())
			return Result.error(ErrorCode.BAD_REQUEST);

		if (userPassword == null)
			return Result.error(ErrorCode.FORBIDDEN);
		Users uc = getUsersClient();

		Log.info("Using Users client: " + uc.getClass().getCanonicalName());
		Log.info("Users server uri: " + ((UsersClient) uc).getServerURI());
		Result<User> owner = uc.getUser(post.getAuthorId(), userPassword);
		if (!owner.isOK()) {
			Log.info("Could not retrieve user information for: " + post.getAuthorId() + " -> " + owner.toString());
			return Result.error(owner.error());
		}
		TX tx = hibernate.beginTransaction();

		if (post.getParentUrl() != null && !post.getParentUrl().isBlank()) {
			String postID = extractResourceID(post.getParentUrl());
			Log.info("Trying to check if parent post exists: " + postID);
			Post p = hibernate.get(tx, Post.class, postID);
			if (p == null) {
				hibernate.abortTransaction(tx);
				return Result.error(ErrorCode.NOT_FOUND);
			}
		}

		post.setCreationTimestamp(System.currentTimeMillis());
		post.setUpVote(0);
		post.setDownVote(0);

		Log.info("Trying to store post");

		while (true) {
			post.setPostId(UUID.randomUUID().toString());
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
						return Result.error(ErrorCode.NOT_FOUND);
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
			synchronized (JavaContent.postLocks) {
				JavaContent.postLocks.put(post.getPostId(), post.getPostId());

				if (post.getParentUrl() != null) {
					String parentId = extractResourceID(post.getParentUrl());
					String lock = JavaContent.postLocks.get(parentId);
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
						+ "(SELECT COUNT(*) FROM Post p2 where p2.parentUrl = CONCAT('" + JavaContent.serverURI
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
			Log.info("Output generated (in this order):");
			for (int i = 0; i < list.size(); i++) {
				Log.info("\t" + list.get(i).toString() + " \ttimestamp: "
						+ hibernate.get(Post.class, list.get(i)).getCreationTimestamp() + " \tReplies: "
						+ this.getPostAnswers(list.get(i), 0).value().size() + " \tUpvotes: "
						+ this.getupVotes(list.get(i)).value());
			}
			return Result.ok(list);
		} catch (Exception e) {
			e.printStackTrace();
			throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public Result<Post> getPost(String postId) {
		Post p = hibernate.get(Post.class, postId);
		Result<Integer> res = this.getupVotes(postId);
		if (res.isOK())
			p.setUpVote(res.value());
		res = this.getDownVotes(postId);
		if (res.isOK())
			p.setDownVote(res.value());

		if (p != null)
			return Result.ok(p);
		else
			return Result.error(ErrorCode.NOT_FOUND);
	}

	@Override
	public Result<List<String>> getPostAnswers(String postId, long maxTimeout) {
		Log.info("Getting Answers for Post " + postId + " maxTimeout=" + maxTimeout);

		Post p = hibernate.get(Post.class, postId);
		if (p == null)
			return Result.error(ErrorCode.NOT_FOUND);

		if (maxTimeout > 0) {
			String lock = null;
			synchronized (JavaContent.postLocks) {
				lock = JavaContent.postLocks.get(postId);
			}

			synchronized (lock) {
				try {
					lock.wait(maxTimeout);
				} catch (InterruptedException e) {
					// Ignore this case...
				}
			}
		}

		String parentURL = serverURI + RestContent.PATH + "/" + postId;

		try {
			List<String> list = null;
			list = hibernate.sql(
					"SELECT p.postId from Post p WHERE p.parentURL='" + parentURL + "' ORDER BY p.creationTimestamp",
					String.class);
			return Result.ok(list);
		} catch (Exception e) {
			e.printStackTrace();
			return Result.error(ErrorCode.INTERNAL_ERROR);
		}
	}

	@Override
	public Result<Post> updatePost(String postId, String userPassword, Post post) {
		TX tx = hibernate.beginTransaction();

		Post p = hibernate.get(tx, Post.class, postId);

		if (p == null) {
			hibernate.abortTransaction(tx);
			return Result.error(ErrorCode.NOT_FOUND);
		}

		if (post.getPostId() != null) {
			hibernate.abortTransaction(tx);
			Log.info("Cannot update post" + postId + ", since the postId cannot be updated");
			return Result.error(ErrorCode.BAD_REQUEST);
		}

		if (post.getAuthorId() != null) {
			hibernate.abortTransaction(tx);
			Log.info("Cannot update post" + postId + ", since the authordId cannot be updated");
			return Result.error(ErrorCode.BAD_REQUEST);
		}

		if (userPassword == null) {
			hibernate.abortTransaction(tx);
			Log.info("Cannot update post" + postId + ", since no user password was provided");
			return Result.error(ErrorCode.FORBIDDEN);
		}

		Result<User> u = this.getUsersClient().getUser(p.getAuthorId(), userPassword);
		if (!u.isOK()) {
			hibernate.abortTransaction(tx);
			return Result.error(u.error());
		}

		// Check if there are answers
		String parentURL = serverURI + RestContent.PATH + "/" + postId;
		if (hibernate.sql(tx, "SELECT p.postId from Post p WHERE p.parentURL='" + parentURL + "'", String.class)
				.size() > 0) {
			hibernate.abortTransaction(tx);
			Log.info("Cannot update post" + postId + ", since there is at least one answer.");
			return Result.error(ErrorCode.BAD_REQUEST);
		}

		// Check if there are votes
		List<Integer> resp = hibernate.sql(tx, "SELECT COUNT(*) from PostVote pv WHERE pv.postId='" + postId + "'",
				Integer.class);
		if (resp.iterator().next() > 0) {
			hibernate.abortTransaction(tx);
			Log.info("Cannot update post" + postId + ", since there is at least one upVote.");
			return Result.error(ErrorCode.BAD_REQUEST);
		}

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
			return Result.error(ErrorCode.BAD_REQUEST);
		}

		return Result.ok(p);
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
			for (String id : descendants){
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
				synchronized (JavaContent.postLocks) {
					String s = JavaContent.postLocks.remove(d.getPostId());
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
			if(p.getMediaUrl() != null)
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
		Log.info("Executing upVote on " + postId + " with Userid:" + userId + " Password: " + userPassword);

		if (userPassword == null)
			return Result.error(ErrorCode.FORBIDDEN);

		Result<User> u = this.getUsersClient().getUser(userId, userPassword);
		if (!u.isOK())
			return Result.error(u.error());

		Log.info("Retrieved user: " + u.value());

		TX tx = hibernate.beginTransaction();

		Post p = hibernate.get(tx, Post.class, postId);

		if (p == null) {
			hibernate.abortTransaction(tx);
			return Result.error(ErrorCode.NOT_FOUND);
		}

		try {
			hibernate.persist(tx, new PostVote(userId, postId, true));
			hibernate.commitTransaction(tx);
		} catch (Exception e) {
			hibernate.abortTransaction(tx);
			return Result.error(ErrorCode.CONFLICT);
		}

		return Result.ok();
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
		Log.info("Executing downVote on " + postId + " with Userid:" + userId + " Password: " + userPassword);

		if (userPassword == null)
			return Result.error(ErrorCode.FORBIDDEN);

		Result<User> u = this.getUsersClient().getUser(userId, userPassword);
		if (!u.isOK())
			return Result.error(u.error());

		Log.info("Retrieved user: " + u.value());

		TX tx = hibernate.beginTransaction();

		Post p = hibernate.get(tx, Post.class, postId);

		if (p == null) {
			hibernate.abortTransaction(tx);
			return Result.error(ErrorCode.NOT_FOUND);
		}

		try {
			hibernate.persist(tx, new PostVote(userId, postId, false));
			hibernate.commitTransaction(tx);
		} catch (Exception e) {
			hibernate.abortTransaction(tx);
			return Result.error(ErrorCode.CONFLICT);
		}

		return Result.ok();
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
		Log.info("Executing getUpVotes on " + postId);
		Post p = hibernate.get(Post.class, postId);
		if (p == null)
			return Result.error(ErrorCode.NOT_FOUND);

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
			return Result.error(ErrorCode.NOT_FOUND);

		List<Integer> count = hibernate.sql(
				"SELECT COUNT(*) from PostVote pv WHERE pv.postId='" + postId + "' AND pv.upVote='false'",
				Integer.class);
		return Result.ok(count.iterator().next());
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
