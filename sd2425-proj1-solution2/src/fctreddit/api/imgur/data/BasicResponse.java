package fctreddit.api.imgur.data;

import java.util.Map;

public class BasicResponse {

	private Map<?,?> data;
	private int status;
	private boolean success;
	
	public Map<?,?> getData() {
		return data;
	}
	
	public void setData(Map<?,?> data) {
		this.data = data;
	}
	
	public int getStatus() {
		return status;
	}
	
	public void setStatus(int status) {
		this.status = status;
	}
	
	public boolean isSuccess() {
		return success;
	}
	
	public void setSuccess(boolean success) {
		this.success = success;
	}
	
}