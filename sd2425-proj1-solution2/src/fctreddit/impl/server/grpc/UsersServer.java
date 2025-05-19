package fctreddit.impl.server.grpc;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.util.logging.Logger;

import fctreddit.impl.server.Discovery;
import fctreddit.impl.server.java.JavaUsers;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;

public class UsersServer {
public static final int PORT = 9000;

	private static final String GRPC_CTX = "/grpc";
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";
	private static final String SERVICE = "Users";
	
	private static Logger Log = Logger.getLogger(UsersServer.class.getName());
	
	public static void main(String[] args) throws Exception {
		//cenas de tls
		String KeyStoreFilename = System.getProperty("javax.net.ssl.KeyStore");
		String KeyStorePassword = System.getProperty("javax.net.ssl.KeyStorePassword");
		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		try(FileInputStream input = new FileInputStream(KeyStoreFilename)){
			keyStore.load(input, KeyStorePassword.toCharArray());
		}
		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
				KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(keyStore, KeyStorePassword.toCharArray());
		
		GrpcUsersServerStub stub = new GrpcUsersServerStub();

		//cenas de tls
		SslContext context = GrpcSslContexts.configure(SslContextBuilder.forServer(keyManagerFactory)).build();
		Server server = NettyServerBuilder.forPort(PORT).addService(stub).sslContext(context).build();
		String serverURI = String.format(SERVER_BASE_URI, InetAddress.getLocalHost().getHostName(), PORT, GRPC_CTX);

		Discovery discovery = new Discovery(Discovery.DISCOVERY_ADDR, SERVICE, serverURI);
		discovery.start();
		JavaUsers.setDiscovery(discovery);
		
		Log.info(String.format("Users gRPC Server ready @ %s\n", serverURI));
		server.start().awaitTermination();
	}
}
