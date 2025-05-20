package fctreddit.impl.server.grpc;

import com.google.protobuf.ByteString;

import fctreddit.api.java.Image;
import fctreddit.api.java.Result;
import fctreddit.api.rest.RestImage;
import fctreddit.impl.grpc.generated_java.ImageGrpc;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.CreateImageArgs;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.CreateImageResult;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.DeleteImageArgs;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.DeleteImageResult;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.GetImageArgs;
import fctreddit.impl.grpc.generated_java.ImageProtoBuf.GetImageResult;
import fctreddit.impl.server.java.JavaImage;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GrpcImageServerStub extends GrpcStub implements ImageGrpc.AsyncService, BindableService {

	private Image impl = new JavaImage();
	
	private static String baseURI = null;
	
	@Override
	public ServerServiceDefinition bindService() {
		return ImageGrpc.bindService(this);
	}
	
	public static void setServerBaseURI(String s) {
		if(GrpcImageServerStub.baseURI == null)
			GrpcImageServerStub.baseURI = s;
	}
	
	@Override
	public void createImage(CreateImageArgs request, StreamObserver<CreateImageResult> responseObserver) {
		Result<String> res = null;
		try {
			res = impl.createImage(request.getUserId(), request.getImageContents().toByteArray()
					, request.getPassword());
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		if (! res.isOK() )
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext(CreateImageResult.newBuilder().setImageId(GrpcImageServerStub.baseURI + 
					RestImage.PATH + "/" + request.getUserId() + "/" + res.value()).build());
			responseObserver.onCompleted();
		}
	}

	@Override
	public void getImage(GetImageArgs request, StreamObserver<GetImageResult> responseObserver) {
		Result<byte[]> res = impl.getImage(request.getUserId(), request.getImageId());
		
		if(! res.isOK() )
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext(GetImageResult.newBuilder().setData(ByteString.copyFrom(res.value())).build());
			responseObserver.onCompleted();
		}
	}

	@Override
	public void deleteImage(DeleteImageArgs request, StreamObserver<DeleteImageResult> responseObserver) {
		Result<Void> res = impl.deleteImage(request.getUserId(), request.getImageId(), request.getPassword());
		
		if(! res.isOK() ) 
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext(DeleteImageResult.getDefaultInstance());
			responseObserver.onCompleted();
		}
	}

}
