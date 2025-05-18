package fctreddit.impl.client.grpc;

import java.net.URI;
import java.util.Iterator;

import com.google.protobuf.ByteString;

import fctreddit.api.java.Result;
import fctreddit.api.java.Result.ErrorCode;
import fctreddit.impl.client.ImageClient;
import fctreddit.impl.grpc.generated_java.ImageGrpc;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.CreateImageArgs;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.CreateImageResult;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.DeleteImageArgs;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.DeleteImageResult;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.GetImageArgs;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.GetImageResult;
import io.grpc.Channel;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.internal.PickFirstLoadBalancerProvider;

public class GrpcImageClient extends ImageClient {

	static {
		LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
	}
	
	final ImageGrpc.ImageBlockingStub stub;
	
	public GrpcImageClient(URI serverURI) {
		super(serverURI);
		Channel channel = ManagedChannelBuilder.forAddress(serverURI.getHost(), serverURI.getPort()).usePlaintext()
				.enableRetry().build();
		stub = ImageGrpc.newBlockingStub(channel);
	}

	@Override
	public Result<String> createImage(String userId, byte[] imageContents, String password) {
		try {
			CreateImageResult res = stub.createImage(CreateImageArgs.newBuilder()
					.setUserId(userId)
					.setImageContents(ByteString.copyFrom(imageContents))
					.setPassword(password)
					.build());
			
			return Result.ok(res.getImageId());
		
		} catch (StatusRuntimeException sre) {
			return Result.error(statusToErrorCode(sre.getStatus()));
		}
	}

	@Override
	public Result<byte[]> getImage(String userId, String imageId) {
		try {
			Iterator<GetImageResult> res = stub.getImage(GetImageArgs.newBuilder()
					.setUserId(userId)
					.setImageId(imageId)
					.build());
			
			ByteString bs = ByteString.empty();
			
			while(res.hasNext()) {
				bs = bs.concat(res.next().getData());
			}
			
			return Result.ok(bs.toByteArray());
		} catch (StatusRuntimeException sre) {
			return Result.error(statusToErrorCode(sre.getStatus()));
		}
	}

	@Override
	public Result<Void> deleteImage(String userId, String imageId, String password) {
		try {
			@SuppressWarnings("unused")
			DeleteImageResult res = stub.deleteImage(DeleteImageArgs
					.newBuilder()
					.setUserId(userId)
					.setImageId(imageId)
					.setPassword(password)
					.build());
			
			return Result.ok();
			
		} catch (StatusRuntimeException sre) {
			return Result.error(statusToErrorCode(sre.getStatus()));
		}
	}
	
	static ErrorCode statusToErrorCode( Status status ) {
    	return switch( status.getCode() ) {
    		case OK -> ErrorCode.OK;
    		case NOT_FOUND -> ErrorCode.NOT_FOUND;
    		case ALREADY_EXISTS -> ErrorCode.CONFLICT;
    		case PERMISSION_DENIED -> ErrorCode.FORBIDDEN;
    		case INVALID_ARGUMENT -> ErrorCode.BAD_REQUEST;
    		case UNIMPLEMENTED -> ErrorCode.NOT_IMPLEMENTED;
    		default -> ErrorCode.INTERNAL_ERROR;
    	};
    }
}
