package fctreddit.impl.server.rest;

import java.util.List;

import fctreddit.api.Post;
import fctreddit.api.java.Content;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestContent;
import fctreddit.impl.server.java.JavaContent;
import jakarta.ws.rs.WebApplicationException;

public class ContentResource extends RestResource implements RestContent {

	private Content impl;
	
	public ContentResource() {
		this.impl = new JavaContent();
	}
	
	@Override
	public String createPost(Post post, String userPassword) {
		Result<String> res = impl.createPost(post, userPassword);
		
		if(res.isOK()) 
			return res.value();
		else
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public List<String> getPosts(long timestamp, String sortOrder) {
		Result<List<String>> res = impl.getPosts(timestamp, sortOrder);
	
		if(res.isOK())
			return res.value();
		else
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public Post getPost(String postId) {
		Result<Post> res = impl.getPost(postId);
		
		if(res.isOK())
			return res.value();
		else
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public List<String> getPostAnswers(String postId, long timeout) {
		Result<List<String>> res = impl.getPostAnswers(postId, timeout);
		
		if(res.isOK()) {
			return res.value();
		} else {
			throw new WebApplicationException(errorCodeToStatus(res.error()));
		}
	}

	@Override
	public Post updatePost(String postId, String userPassword, Post post) {
		Result<Post> res = impl.updatePost(postId, userPassword, post);
		
		if(res.isOK()) 
			return res.value();
		else 
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public void deletePost(String postId, String userPassword) {
		Result<Void> res = impl.deletePost(postId, userPassword);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public void upVotePost(String postId, String userId, String userPassword) {
		Result<Void> res = impl.upVotePost(postId, userId, userPassword);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public void removeUpVotePost(String postId, String userId, String userPassword) {
		Result<Void> res = impl.removeUpVotePost(postId, userId, userPassword);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public void downVotePost(String postId, String userId, String userPassword) {
		Result<Void> res = impl.downVotePost(postId, userId, userPassword);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public void removeDownVotePost(String postId, String userId, String userPassword) {
		Result<Void> res = impl.removeDownVotePost(postId, userId, userPassword);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
	}

	@Override
	public Integer getupVotes(String postId) {
		Result<Integer> res = impl.getupVotes(postId);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
		
		return res.value();
	}

	@Override
	public Integer getDownVotes(String postId) {
		Result<Integer> res = impl.getDownVotes(postId);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));
		
		return res.value();
	}
	
	@Override
	public void removeTracesOfUser(String userId) {
		Result<Void> res = impl.removeTracesOfUser(userId);
		
		if(!res.isOK())
			throw new WebApplicationException(errorCodeToStatus(res.error()));

	}
}
