package fctreddit.impl.server.java;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import fctreddit.api.User;
import fctreddit.api.java.Image;
import fctreddit.api.java.Result;
import fctreddit.api.java.Result.ErrorCode;
import fctreddit.impl.kafka.KafkaPublisher;

public class JavaImage extends JavaServer implements Image {


    private static Logger Log = Logger.getLogger(JavaImage.class.getName());

    private static final Path baseDirectory = Path.of("home", "sd", "images");
    private static final String baseUrl;

    static {
        String hostname = "localhost"; // fallback
        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            e.printStackTrace();
        }
        baseUrl = "https://" + hostname + ":8080/rest/image/";
    }
    private static final Map<String, Integer> refCount = new ConcurrentHashMap<>();
    private static final Map<String, Long> createdAt = new ConcurrentHashMap<>();
    private static KafkaPublisher publisher;

    public JavaImage() {
        File f = baseDirectory.toFile();

        if (!f.exists()) {
            f.mkdirs();
        }
    }

    public static void setKafka(KafkaPublisher p) {
        if (JavaImage.publisher == null) {
            JavaImage.publisher = p;
            Log.info("Kafka publisher has been set.");
        } else {
            Log.warning("Kafka publisher already set, ignoring reinitialization.");
        }
    }

    public static void incrementRef(String imageId, boolean isCreate) {
        Log.info("Current keys in refCount: " + refCount.keySet());
        if (isCreate) {
            refCount.merge(imageId, 1, Integer::sum);
            Log.info("merged + " + imageId);
        }
        else {
            if (refCount.containsKey(imageId)) {
                int current = refCount.get(imageId);
                if (current > 1) {
                    Log.info("Updating + " + imageId);
                    refCount.put(imageId, current - 1);
                }
                else {
                    Log.info("Removing Ref + " + imageId);
                    refCount.remove(imageId);
                }

            }
        }
    }

    @Override
    public Result<String> createImage(String userId, byte[] imageContents, String password) {
        Result<User> owner = getUsersClient().getUser(userId, password);
        if (!owner.isOK())
            return Result.error(owner.error());

        String id = null;
        Path image = null;

        // check if user directory exists
        Path userDirectory = Path.of(baseDirectory.toString(), userId);
        File uDir = userDirectory.toFile();
        if (!uDir.exists()) {
            uDir.mkdirs();
        }

        synchronized (this) {
            while (true) {
                id = UUID.randomUUID().toString();
                image = Path.of(userDirectory.toString(), id);
                File iFile = image.toFile();

                if (!iFile.exists())
                    break;
            }
            try {
                Files.write(image, imageContents);
            } catch (IOException e) {
                e.printStackTrace();
                return Result.error(ErrorCode.INTERNAL_ERROR);
            }
        }
        String aa = userId + "/" + id;
        Log.info("Created image with id " + id + " for user " + userId);

        createdAt.put(aa, System.currentTimeMillis());
        return Result.ok(id);
    }

    @Override
    public Result<byte[]> getImage(String userId, String imageId) {
        Log.info("Get image with id " + imageId + " owned by user " + userId);

        Path image = Path.of(baseDirectory.toString(), userId, imageId);
        File iFile = image.toFile();

        synchronized (this) {
            if (iFile.exists() && iFile.isFile()) {
                try {
                    return Result.ok(Files.readAllBytes(image));
                } catch (IOException e) {
                    e.printStackTrace();
                    return Result.error(ErrorCode.INTERNAL_ERROR);
                }
            } else {
                System.out.println("imagem n encontrada");
                return Result.error(ErrorCode.NOT_FOUND);
            }
        }

    }

    @Override
    public Result<Void> deleteImage(String userId, String imageId, String password) {
        Log.info("Delete image with id " + imageId + " owned by user " + userId);

        Result<User> owner = getUsersClient().getUser(userId, password);

        if (!owner.isOK()) {
            Log.info("Failed to authenticate user: " + owner.error());
            return Result.error(owner.error());
        }

        Path image = Path.of(baseDirectory.toString(), userId, imageId);
        File iFile = image.toFile();

        synchronized (this) {
            if (iFile.exists() && iFile.isFile()) {
                iFile.delete();
                String url = baseUrl  + userId + "/" + imageId;
                if (publisher != null) {
                    publisher.publish("images", url); // also fix topic name if needed
                } else {
                    Log.severe("Kafka publisher is null in deleteImage!");
                }

                return Result.ok();
            } else {
                return Result.error(ErrorCode.NOT_FOUND);
            }
        }
    }

    public static void handleImageDeletion() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    Long now = System.currentTimeMillis();
                    System.out.println("estou de facto aqui ola!!!!!!");
                    for (String id : new HashSet<>(createdAt.keySet())) {
                        Long at = createdAt.get(id);
                        boolean unused = !refCount.containsKey(id);
                        boolean thirtyPassed = at != null && (now - at) >= 30000;

                        System.out.println(id + " com " + refCount.get(id));

                        if (unused && thirtyPassed) {
                            System.out.println("estou de facto aqui 2");
                            String[] parts = id.split("/");

                            String userId = parts[0];
                            String imageId = parts[1];

                            Path image = Path.of(baseDirectory.toString(), userId, imageId);
                            File iFile = image.toFile();
                            System.out.println("Vou tentar apagar o ficheiro: " + image);
                            System.out.println(image + " Existe? " + iFile.exists() + ", É ficheiro? " + iFile.isFile());
                            synchronized (JavaImage.class) {
                                if (iFile.exists() && iFile.isFile()) {
                                    iFile.delete();
                                    createdAt.remove(id); // limpar para evitar duplicação futura
                                    System.out.println("imagem foi apagada? " + !iFile.exists());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error: " + e.getMessage());
                }
            }
        }).start();
    }

}
