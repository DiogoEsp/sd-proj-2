package fctreddit.impl.server.rest;

import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

import fctreddit.impl.kafka.KafkaPublisher;
import fctreddit.impl.kafka.KafkaSubscriber;
import fctreddit.impl.kafka.KafkaUtils;
import fctreddit.impl.kafka.RecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import fctreddit.impl.server.Discovery;
import fctreddit.impl.server.java.JavaImage;

import javax.net.ssl.SSLContext;

public class ImageServer {

    private static Logger Log = Logger.getLogger(ImageServer.class.getName());

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
    }

    public static final int PORT = 8080;
    public static final String SERVICE = "Image";
    private static final String SERVER_URI_FMT = "https://%s:%s/rest";


    public static void main(String[] args) {
        try {

            ResourceConfig config = new ResourceConfig();
            config.register(ImageResource.class);

            String hostname = InetAddress.getLocalHost().getHostName();
            String serverURI = String.format(SERVER_URI_FMT, hostname, PORT);
            ImageResource.setServerBaseURI(serverURI);

            JdkHttpServerFactory.createHttpServer(URI.create(serverURI), config, SSLContext.getDefault());

            Log.info(String.format("%s Server ready @ %s\n", SERVICE, serverURI));

            Discovery d = new Discovery(Discovery.DISCOVERY_ADDR, SERVICE, serverURI);
            JavaImage.setDiscovery(d);
            d.start();

            KafkaUtils.createTopic("posts");
            KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber("kafka:9092", List.of("posts"));

            subscriber.start(new RecordProcessor() {
                @Override
                public void onReceive(ConsumerRecord<String, String> r) {
                    try {
                        String[] value = r.value().split(" ");
                        String operation = value[0];
                        String mediaUrl = value[1];
                        String[] bMediaUrl = mediaUrl.split("/");
                        // remove extensão se existir (fica só o UUID)
                        int dot = bMediaUrl[bMediaUrl.length - 1].indexOf('.');
                        String cleanImageId = (dot == -1) ? bMediaUrl[bMediaUrl.length - 1]
                                : bMediaUrl[bMediaUrl.length - 1].substring(0, dot);
                        String all = bMediaUrl[bMediaUrl.length - 2] + "/" + cleanImageId;

                        System.out.println("Formatted Version: " + operation + " " + all);
                        switch (operation) {
                            case "create" -> JavaImage.incrementRef(all, true);
                            case "delete" -> JavaImage.incrementRef(all, false);
                        }
                    } catch (Exception e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                }
            });
            JavaImage.handleImageDeletion();

            KafkaUtils.createTopic("image");
            KafkaPublisher pub = KafkaPublisher.createPublisher("kafka:9092");
            JavaImage.setKafka(pub);

        } catch (Exception e) {
            Log.severe(e.getMessage());
        }


    }
}
