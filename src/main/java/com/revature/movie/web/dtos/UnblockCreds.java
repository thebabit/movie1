package com.revature.movie.web.dtos;

import com.revature.movie.model.UserRole;

import java.util.Objects;

public class UnblockCreds {

    private String username;
    private UserRole role;

    public UnblockCreds() {
        super();
    }

    public UnblockCreds(String username, UserRole role) {
        this.username = username;
        this.role = role;
    }


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public UserRole getRole() {
        return role;
    }

    public void setRole(UserRole role) {
        this.role = role;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnblockCreds that = (UnblockCreds) o;
        return Objects.equals(username, that.username) &&
                role == that.role;
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, role);
    }

    @Override
    public String toString() {
        return "UnblockCreds{" +
                "username='" + username + '\'' +
                ", role=" + role +
                '}';
    }
}
