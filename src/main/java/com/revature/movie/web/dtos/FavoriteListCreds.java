package com.revature.movie.web.dtos;

import java.util.Objects;

public class FavoriteListCreds {
   private String movieName;
    private String apiId;
    private int userId;

    public FavoriteListCreds() {super();
    }

    public FavoriteListCreds(String movieName, String apiId, int userId) {
        this.movieName = movieName;
        this.apiId = apiId;
        this.userId = userId;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public String getApiId() {
        return apiId;
    }

    public void setApiId(String apiId) {
        this.apiId = apiId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FavoriteListCreds that = (FavoriteListCreds) o;
        return Objects.equals(movieName, that.movieName) &&
                Objects.equals(apiId, that.apiId) &&
                Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieName, apiId, userId);
    }

    @Override
    public String toString() {
        return "FavoriteListCreds{" +
                "movieName='" + movieName + '\'' +
                ", apiId='" + apiId + '\'' +
                ", userId='" + userId + '\'' +
                '}';
    }
}
