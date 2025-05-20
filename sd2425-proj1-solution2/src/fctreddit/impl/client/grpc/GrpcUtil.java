package fctreddit.impl.client.grpc;

import io.grpc.Channel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;

public class GrpcUtil {

    public static Channel buildChannel(URI serverURI) throws Exception {
        String trustStoreFilename = System.getProperty("javax.net.ssl.trustStore");
        String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try(FileInputStream input = new FileInputStream(trustStoreFilename)) {
            trustStore.load(input, trustStorePassword.toCharArray());
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SslContext context = GrpcSslContexts
                .configure(
                        SslContextBuilder.forClient().trustManager(trustManagerFactory)
                ).build();

        return NettyChannelBuilder
                .forAddress(serverURI.getHost(), serverURI.getPort())
                .sslContext(context)
                .enableRetry()
                .build();

    }
}
