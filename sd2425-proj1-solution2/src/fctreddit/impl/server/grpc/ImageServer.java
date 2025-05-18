package fctreddit.impl.server.grpc;

import java.net.InetAddress;
import java.util.logging.Logger;

import fctreddit.impl.server.Discovery;
import fctreddit.impl.server.java.JavaImage;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;

public class ImageServer {
public static final int PORT = 9000;

	private static final String GRPC_CTX = "/grpc";
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";
	private static final String SERVICE = "Image";
	
	private static Logger Log = Logger.getLogger(ImageServer.class.getName());
	
	public static void main(String[] args) throws Exception {
		
		GrpcImageServerStub stub = new GrpcImageServerStub();
		
		ServerCredentials cred = InsecureServerCredentials.create();
		Server server = Grpc.newServerBuilderForPort(PORT, cred) .addService(stub).build();
		String serverURI = String.format(SERVER_BASE_URI, InetAddress.getLocalHost().getHostAddress(), PORT, GRPC_CTX);
		
		GrpcImageServerStub.setServerBaseURI(serverURI);
		
		Discovery discovery = new Discovery(Discovery.DISCOVERY_ADDR, SERVICE, serverURI);
		discovery.start();
		JavaImage.setDiscovery(discovery);
		
		Log.info(String.format("Image gRPC Server ready @ %s\n", serverURI));
		server.start().awaitTermination();
	}
}
