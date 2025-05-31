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
import fctreddit.impl.server.java.JavaContent;

import javax.net.ssl.SSLContext;

public class ContentServer {

	private static Logger Log = Logger.getLogger(ContentServer.class.getName());

	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}
	
	public static final int PORT = 8080;
	public static final String SERVICE = "Content";
	private static final String SERVER_URI_FMT = "https://%s:%s/rest";

	public static void main(String[] args) {
		try {
			
			ResourceConfig config = new ResourceConfig();
			config.register(ContentResource.class);
	
			String hostname = InetAddress.getLocalHost().getHostName();
			String serverURI = String.format(SERVER_URI_FMT, hostname, PORT);
			
			JavaContent.setServerURI(serverURI);
			
			JdkHttpServerFactory.createHttpServer( URI.create(serverURI), config, SSLContext.getDefault());
		
			Log.info(String.format("%s Server ready @ %s\n",  SERVICE, serverURI));
			
			Discovery d = new Discovery(Discovery.DISCOVERY_ADDR, SERVICE, serverURI);
			JavaContent.setDiscovery(d);
			d.start();

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

		} catch( Exception e) {
			Log.severe(e.getMessage());
		}
	}	
}
