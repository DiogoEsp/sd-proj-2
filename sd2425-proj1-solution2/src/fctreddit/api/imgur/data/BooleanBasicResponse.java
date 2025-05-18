package fctreddit.api.imgur.data;

public class BooleanBasicResponse {

	private boolean data;
	private int status;
	private boolean success;
	
	public boolean getData() {
		return data;
	}
	
	public void setData(boolean data) {
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