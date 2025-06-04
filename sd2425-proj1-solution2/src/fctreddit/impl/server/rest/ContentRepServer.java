package fctreddit.impl.server.rest;

import fctreddit.impl.kafka.KafkaPublisher;
import fctreddit.impl.kafka.KafkaSubscriber;
import fctreddit.impl.kafka.KafkaUtils;
import fctreddit.impl.kafka.RecordProcessor;
import fctreddit.impl.server.Discovery;
import fctreddit.impl.server.java.JavaContentRep;
import fctreddit.impl.server.rest.replication.ContentRepResource;
import fctreddit.impl.server.rest.filter.VersionFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

public class ContentRepServer {

    private static Logger Log = Logger.getLogger(ContentRepServer.class.getName());

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
    }

    public static final int PORT = 8080;
    public static final String SERVICE = "Content";
    private static final String SERVER_URI_FMT = "https://%s:%s/rest";

    public static void main(String[] args) {

        KafkaUtils.createTopic("posts");

        KafkaPublisher pub = KafkaPublisher.createPublisher("kafka:9092");
        JavaContentRep.setKafka(pub);

        KafkaUtils.createTopic("images");
        KafkaSubscriber sub = KafkaSubscriber.createSubscriber("kafka:9092", List.of("images"));

        KafkaUtils.createTopic("replication");
        KafkaPublisher repPub = KafkaPublisher.createPublisher("kafka:9092");
        JavaContentRep.setKafkaRep(repPub);
        System.out.println("Setted publisher");

        KafkaSubscriber repSub = KafkaSubscriber.createSubscriber("kafka:9092", List.of("replication"));
        System.out.println("Setted subscriber");



        try {
            ResourceConfig config = new ResourceConfig();
            config.register(VersionFilter.class);
            config.register(ContentRepResource.class);

            String hostname = InetAddress.getLocalHost().getHostName();
            String serverURI = String.format(SERVER_URI_FMT, hostname, PORT);
            JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, SSLContext.getDefault());



            Log.info(String.format("%s Replication Server ready @ %s\n", SERVICE, serverURI));

            Discovery d = new Discovery(Discovery.DISCOVERY_ADDR, SERVICE, serverURI);
            JavaContentRep.setDiscovery(d);
            JavaContentRep rep = JavaContentRep.getInstance();
            d.start();
            repSub.start(new RecordProcessor() {
                @Override
                public void onReceive(ConsumerRecord<String, String> record) {
                    Log.info("Handling Replication!");
                    rep.handleReplication(record);
                }
            });

            sub.start(new RecordProcessor() {
                @Override
                public void onReceive(ConsumerRecord<String, String> r) {
                    try {
                        Log.info("Operation deleteImages with subscriber");
                        String value = r.value();
                        rep.handleDeletedImages(value);
                    } catch (Exception e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                }
            });
            //More code can be executed here...
        } catch (Exception e) {
            Log.severe(e.getMessage());
        }
    }
}
