package fctreddit.impl.client;

import java.net.URI;
import java.util.logging.Logger;

import fctreddit.api.java.Result;
import fctreddit.api.java.Result.ErrorCode;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation.Builder;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

public abstract class Client {

	protected static final int READ_TIMEOUT = 5000;
	protected static final int CONNECT_TIMEOUT = 5000;

	protected static final int MAX_RETRIES = 10;
	protected static final int RETRY_SLEEP = 5000;
	
	private static Logger Log = Logger.getLogger(Client.class.getName());

	private final URI serverURI;
	
	public Client(URI serverURI) {
		this.serverURI = serverURI;
	}
	
	public URI getServerURI () {
		return this.serverURI;
	}
	
	protected Response executeGet(Builder b) {
		for(int i = 0; i < MAX_RETRIES ; i++) {
			try {
				return b.get();
			} catch( ProcessingException x ) {
				Log.info(x.getMessage());
				
				try {
					Thread.sleep(RETRY_SLEEP);
				} catch (InterruptedException e) {
					//Nothing to be done here.
				}
			}
			catch( Exception x ) {
				x.printStackTrace();
			}
		}
		return null;
	}
	
	protected Response executeDelete(Builder b) {
		for(int i = 0; i < MAX_RETRIES ; i++) {
			try {
				return b.delete();
			} catch( ProcessingException x ) {
				Log.info(x.getMessage());
				
				try {
					Thread.sleep(RETRY_SLEEP);
				} catch (InterruptedException e) {
					//Nothing to be done here.
				}
			}
			catch( Exception x ) {
				x.printStackTrace();
			}
		}
		return null;
	}
	
	protected Response executePost(Builder b, Entity<?> ent) {
		for(int i = 0; i < MAX_RETRIES ; i++) {
			try {
				return b.post(ent);
			} catch( ProcessingException x ) {
				Log.info(x.getMessage());
				
				try {
					Thread.sleep(RETRY_SLEEP);
				} catch (InterruptedException e) {
					//Nothing to be done here.
				}
			}
			catch( Exception x ) {
				x.printStackTrace();
			}
		}
		return null;
	}
	
	protected Response executePut(Builder b, Entity<?> ent) {
		for(int i = 0; i < MAX_RETRIES ; i++) {
			try {
				return b.put(ent);
			} catch( ProcessingException x ) {
				Log.info(x.getMessage());
				
				try {
					Thread.sleep(RETRY_SLEEP);
				} catch (InterruptedException e) {
					//Nothing to be done here.
				}
			}
			catch( Exception x ) {
				x.printStackTrace();
			}
		}
		return null;
	}
	
	protected <T> Result<T> extractResponseWithBody(Response r, Class<T> c) {
		if( r.getStatus() != Status.OK.getStatusCode() )
			return Result.error( getErrorCodeFrom(r.getStatus()));
		else
			return Result.ok( r.readEntity( c ));	
	} 
	
	protected <T> Result<T> extractResponseWithBody(Response r, GenericType<T> t) {
		if( r.getStatus() != Status.OK.getStatusCode() )
			return Result.error( getErrorCodeFrom(r.getStatus()));
		else
			return Result.ok( r.readEntity( t ));	
	} 
	
	protected Result<Void> extractResponseWithoutBody(Response r) {
		if( r.getStatus() != Status.NO_CONTENT.getStatusCode() )
			return Result.error( getErrorCodeFrom(r.getStatus()));
		else
			return Result.ok();	
	} 
	
	public static ErrorCode getErrorCodeFrom(int status) {
		return switch (status) {
		case 200, 209 -> ErrorCode.OK;
		case 409 -> ErrorCode.CONFLICT;
		case 403 -> ErrorCode.FORBIDDEN;
		case 404 -> ErrorCode.NOT_FOUND;
		case 400 -> ErrorCode.BAD_REQUEST;
		case 500 -> ErrorCode.INTERNAL_ERROR;
		case 501 -> ErrorCode.NOT_IMPLEMENTED;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}
}
