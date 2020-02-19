package com.revature.movie.exceptions;

public class QuizzardException extends RuntimeException {

    public QuizzardException(String message) {
        super(message);
    }

    public QuizzardException(String message, Throwable cause) {
        super(message, cause);
    }

    public QuizzardException(Throwable cause) {
        super(cause);
    }

}
