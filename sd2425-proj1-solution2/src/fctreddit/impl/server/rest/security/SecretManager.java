package fctreddit.impl.server.rest.security;

public class SecretManager {

    private static final SecretManager INSTANCE = new SecretManager();

    private String secret;

    public static SecretManager getInstance() {
        return INSTANCE;
    }

    private SecretManager() {
        this.secret = null;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getSecret() {
        return secret;
    }

    public boolean isValid(String other) {
        return other != null && other.equals(this.secret);
    }

}
