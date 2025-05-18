package fctreddit.impl.server.grpc;

import java.util.List;
import java.util.logging.Logger;

import fctreddit.api.User;
import fctreddit.api.java.Result;
import fctreddit.api.java.Users;
import fctreddit.impl.client.grpc.DataModelAdaptor;
import fctreddit.impl.grpc.generated_java.UsersGrpc;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.CreateUserArgs;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.CreateUserResult;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.DeleteUserArgs;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.DeleteUserResult;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.GetUserArgs;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.GetUserResult;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.GrpcUser;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.SearchUserArgs;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.UpdateUserArgs;
import fctreddit.impl.grpc.generated_java.UsersProtoBuf.UpdateUserResult;
import fctreddit.impl.server.java.JavaUsers;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;


public class GrpcUsersServerStub extends GrpcStub implements UsersGrpc.AsyncService, BindableService{

	private static Logger Log = Logger.getLogger(GrpcUsersServerStub.class.getName());
	
	Users impl = new JavaUsers();
	
	 @Override 
	 public final ServerServiceDefinition bindService() {
	      return UsersGrpc.bindService(this);
	 }

	@Override
    public void createUser(CreateUserArgs request, StreamObserver<CreateUserResult> responseObserver) {
		Log.info("Create User: " + request);
		
    	Result<String> res = impl.createUser( DataModelAdaptor.GrpcUser_to_User(request.getUser()));	
    	if( ! res.isOK() ) 
    		responseObserver.onError(errorCodeToStatus(res.error()));
    	else {
			responseObserver.onNext( CreateUserResult.newBuilder().setUserId( res.value() ).build());
			responseObserver.onCompleted();
    	}
    }

	@Override
    public void getUser(GetUserArgs request, StreamObserver<GetUserResult> responseObserver) {
		Log.info("Get User: " + request);
		
		Result<User> res = impl.getUser(request.getUserId(), request.getPassword());
		if( ! res.isOK() )
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext( GetUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(res.value())).build() );
			responseObserver.onCompleted();
		}
    }

	@Override
    public void updateUser(UpdateUserArgs request, StreamObserver<UpdateUserResult> responseObserver) {
		Log.info("Update User: " + request);
		
		Result<User> res = impl.updateUser(request.getUserId(), request.getPassword(), 
								DataModelAdaptor.GrpcUser_to_User(request.getUser()));
		if( ! res.isOK() )
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext( UpdateUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(res.value())).build());
			responseObserver.onCompleted();
		}
   }

	@Override
    public void deleteUser(DeleteUserArgs request, StreamObserver<DeleteUserResult> responseObserver) {
		Log.info("Delete User: " + request);
		
		Result<User> res = impl.deleteUser(request.getUserId(), request.getPassword());
		
		if ( ! res.isOK() )
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			responseObserver.onNext( DeleteUserResult.newBuilder().setUser(DataModelAdaptor.User_to_GrpcUser(res.value())).build());
			responseObserver.onCompleted();
		}
    }

	
	@Override
    public void searchUsers(SearchUserArgs request, StreamObserver<GrpcUser> responseObserver) {
		Log.info("Search Users: " + request);
		
		Result<List<User>> res = impl.searchUsers(request.getPattern());
		
		if( ! res.isOK() )
			responseObserver.onError(errorCodeToStatus(res.error()));
		else {
			for(User u: res.value()) {
				responseObserver.onNext( DataModelAdaptor.User_to_GrpcUser(u));
			}
			responseObserver.onCompleted();
		}
  }

}
