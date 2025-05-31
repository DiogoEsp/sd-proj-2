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

    private static final Map<String, Integer> refCount = new ConcurrentHashMap<>();
    private static final Map<String, Long> createdAt = new ConcurrentHashMap<>();
    private static KafkaPublisher publisher;

    public JavaImage() {
        File f = baseDirectory.toFile();

        if (!f.exists()) {
            f.mkdirs();
        }
    }

    public static void setKafka(KafkaPublisher publisher) {
        if (JavaImage.publisher == null)
            JavaImage.publisher = publisher;
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
                publisher.publish("images", imageId);
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
                    Thread.sleep(15000);
                    Long now = System.currentTimeMillis();
                    System.out.println("estou de facto aqui ola!!!!!!");
                    for (String id : new HashSet<>(createdAt.keySet())) {
                        Long at = createdAt.get(id);
                        boolean unused = !refCount.containsKey(id);
                        boolean thirtyPassed = at != null && (now - at) >= 30000;

                        if (unused && thirtyPassed) {
                            System.out.println("estou de facto aqui 2");
                            String[] parts = id.split("/");

                            String userId = parts[0];
                            String imageId = parts[1];

                            Path image = Path.of(baseDirectory.toString(), userId, imageId);
                            File iFile = image.toFile();
                            System.out.println("Vou tentar apagar o ficheiro: " + image);
                            System.out.println("Existe? " + iFile.exists() + ", É ficheiro? " + iFile.isFile());
                            synchronized (JavaImage.class) {
                                if (iFile.exists() && iFile.isFile()) {
                                    iFile.delete();
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
