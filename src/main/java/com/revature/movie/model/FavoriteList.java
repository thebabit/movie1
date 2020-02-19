package com.revature.movie.model;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "FAVORITE_LIST")
@SequenceGenerator(name = "favorite_gen", sequenceName = "favorite_seq", allocationSize = 1)
public class FavoriteList {

    @Id
    @Column(name = "FAVORITE_ID")
    @GeneratedValue(strategy=GenerationType.SEQUENCE, generator="favorite_gen")
    private int favoriteId;



    @Column(name = "MOVIE_NAME", nullable = false)
    private String movieName;

    @Column( name = "API_ID", nullable = false)
    private String apiId;


    public FavoriteList() {
        super();
    }

    public FavoriteList(String movieName, String apiId) {
        this.movieName = movieName;
        this.apiId = apiId;
    }

    public FavoriteList(int favoriteId,  String movieName, String apiId) {
        this.favoriteId = favoriteId;
        this.movieName = movieName;
        this.apiId = apiId;
    }

    public FavoriteList(String movieName) {
        this.movieName = movieName;
    }

    public int getFavoriteId() {
        return favoriteId;
    }

    public void setFavoriteId(int favoriteId) {
        this.favoriteId = favoriteId;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FavoriteList that = (FavoriteList) o;
        return favoriteId == that.favoriteId &&
                apiId == that.apiId &&
                Objects.equals(movieName, that.movieName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(favoriteId, movieName, apiId);
    }

    @Override
    public String toString() {
        return "FavoriteList{" +
                "favoriteId=" + favoriteId +
                ", movieName='" + movieName + '\'' +
                ", apiId=" + apiId +
                '}';
    }
}
