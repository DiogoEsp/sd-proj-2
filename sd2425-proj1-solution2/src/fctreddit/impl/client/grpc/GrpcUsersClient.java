package fctreddit.impl.client.grpc;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import fctreddit.api.User;
import fctreddit.api.java.Result;
import fctreddit.api.java.Result.ErrorCode;
import fctreddit.impl.client.UsersClient;
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
import io.grpc.Channel;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.internal.PickFirstLoadBalancerProvider;


public class GrpcUsersClient extends UsersClient {

    static {
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
    }

    final UsersGrpc.UsersBlockingStub stub;

    public GrpcUsersClient(URI serverURI) {
        super(serverURI);
        Channel channel = null;
        try {
            channel = GrpcUtil.buildChannel(serverURI);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        stub = UsersGrpc.newBlockingStub(channel);
    }

    @Override
    public Result<String> createUser(User user) {
        try {
            CreateUserResult res = stub.createUser(CreateUserArgs.newBuilder()
                    .setUser(DataModelAdaptor.User_to_GrpcUser(user))
                    .build());

            return Result.ok(res.getUserId());
        } catch (StatusRuntimeException sre) {
            return Result.error(statusToErrorCode(sre.getStatus()));
        }
    }

    @Override
    public Result<User> getUser(String userId, String password) {
        try {
            GetUserResult res = stub.getUser(GetUserArgs.newBuilder()
                    .setUserId(userId).setPassword(password)
                    .build());

            return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
        } catch (StatusRuntimeException sre) {
            return Result.error(statusToErrorCode(sre.getStatus()));
        }
    }

    @Override
    public Result<User> updateUser(String userId, String password, User user) {
        try {
            UpdateUserResult res = stub.updateUser(UpdateUserArgs.newBuilder()
                    .setUserId(userId)
                    .setPassword(password)
                    .setUser(DataModelAdaptor.User_to_GrpcUser(user))
                    .build());

            return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
        } catch (StatusRuntimeException sre) {
            return Result.error(statusToErrorCode(sre.getStatus()));
        }
    }

    @Override
    public Result<User> deleteUser(String userId, String password) {
        try {
            DeleteUserResult res = stub.deleteUser(DeleteUserArgs.newBuilder()
                    .setUserId(userId)
                    .setPassword(password)
                    .build());

            return Result.ok(DataModelAdaptor.GrpcUser_to_User(res.getUser()));
        } catch (StatusRuntimeException sre) {
            return Result.error(statusToErrorCode(sre.getStatus()));
        }
    }

    @Override
    public Result<List<User>> searchUsers(String pattern) {
        try {
            Iterator<GrpcUser> res = stub.searchUsers(SearchUserArgs.newBuilder()
                    .setPattern(pattern)
                    .build());

            List<User> ret = new ArrayList<User>();
            while (res.hasNext()) {
                ret.add(DataModelAdaptor.GrpcUser_to_User(res.next()));
            }
            return Result.ok(ret);
        } catch (StatusRuntimeException sre) {
            return Result.error(statusToErrorCode(sre.getStatus()));
        }
    }

    static ErrorCode statusToErrorCode(Status status) {
        return switch (status.getCode()) {
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
