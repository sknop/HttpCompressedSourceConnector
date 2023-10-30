package io.confluent.bootcamp;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

public final class BasicAuthenticator extends Authenticator {

    private final String username;
    private final String password;

    BasicAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(username, password.toCharArray());
    }
}
