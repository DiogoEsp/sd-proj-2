package fctreddit.impl.server.grpc;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.List;
import java.util.logging.Logger;

import fctreddit.impl.kafka.KafkaPublisher;
import fctreddit.impl.kafka.KafkaSubscriber;
import fctreddit.impl.kafka.KafkaUtils;
import fctreddit.impl.kafka.RecordProcessor;
import fctreddit.impl.server.Discovery;
import fctreddit.impl.server.java.JavaContent;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.net.ssl.KeyManagerFactory;

public class ContentServer {
    public static final int PORT = 9000;

    private static final String GRPC_CTX = "/grpc";
    private static final String SERVER_BASE_URI = "grpc://%s:%s%s";
    private static final String SERVICE = "Content";

    private static Logger Log = Logger.getLogger(ContentServer.class.getName());

    public static void main(String[] args) throws Exception {

        String KeyStoreFilename = System.getProperty("javax.net.ssl.keyStore");
        String KeyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());

        try (FileInputStream input = new FileInputStream(KeyStoreFilename)) {
            keyStore.load(input, KeyStorePassword.toCharArray());
        }

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KeyStorePassword.toCharArray());

        GrpcContentServerStub stub = new GrpcContentServerStub();

        SslContext context = GrpcSslContexts.configure(SslContextBuilder.forServer(keyManagerFactory)).build();
        Server server = NettyServerBuilder.forPort(PORT).addService(stub).sslContext(context).build();
        String serverURI = String.format(SERVER_BASE_URI, InetAddress.getLocalHost().getHostName(), PORT, GRPC_CTX);

        JavaContent.setServerURI(serverURI);

        Discovery discovery = new Discovery(Discovery.DISCOVERY_ADDR, SERVICE, serverURI);
        discovery.start();
        JavaContent.setDiscovery(discovery);

        KafkaUtils.createTopic("posts");

        KafkaPublisher pub = KafkaPublisher.createPublisher("kafka:9092");
        JavaContent.setKafka(pub);
        Log.info("crio bem");


        KafkaUtils.createTopic("images");
        KafkaSubscriber sub = KafkaSubscriber.createSubscriber("kafka:9092", List.of("images"));

        sub.start(new RecordProcessor() {
            @Override
            public void onReceive(ConsumerRecord<String, String> r) {
                try{
                    String value = r.value().toString();
                    JavaContent.handleDeletedImages(value);
                }catch(Exception e){
                    System.out.println("Error: " + e.getMessage());
                }
            }
        });

        Log.info(String.format("Image gRPC Server ready @ %s\n", serverURI));
        server.start().awaitTermination();
    }
}
