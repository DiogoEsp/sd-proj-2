package fctreddit.api;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class PostVote {

	@Id
	private String userId;
	@Id
	private String postId;
	private boolean upVote;
	
	public PostVote() {
		this.userId = null;
		this.postId = null;
		this.upVote = true;
	}
	
	public PostVote(String userId, String postId, boolean upVote) {
		this.userId = userId;
		this.postId = postId;
		this.upVote = upVote;
	}
	
	public String getUserId() {
		return this.userId;
	}
	
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	public String getPostId() {
		return this.postId;
	}
	
	public void setPostId(String postId) {
		this.postId = postId;
	}
	
	public boolean getUpVote() {
		return this.upVote;
	}
	
	public void setUpVote(boolean upVote) {
		this.upVote = upVote;
	}
	
	public boolean isUpVote() {
		return this.upVote;
	}
	
	public boolean isDownVote() {
		return !this.upVote;
	}
 	
}
