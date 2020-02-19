package com.revature.movie.exceptions;

public class AuthenticationException extends QuizzardException {

    public AuthenticationException() {
        super("Authentication failed!");
    }

    public AuthenticationException(String message) {
        super(message);
    }

}
