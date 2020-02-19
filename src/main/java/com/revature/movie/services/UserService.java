package com.revature.movie.services;

import com.revature.movie.exceptions.AuthenticationException;
import com.revature.movie.exceptions.BadRequestException;
import com.revature.movie.model.FavoriteList;
import com.revature.movie.model.User;
import com.revature.movie.model.UserRole;
import com.revature.movie.repos.UserRepos;

import com.revature.movie.web.dtos.Credentials;
import com.revature.movie.web.dtos.FavoriteListCreds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class UserService {

    private UserRepos userRepo;

    @Autowired
    public UserService(UserRepos repo) {
        super();
        this.userRepo = repo;
    }

    @Transactional(readOnly=true)
    public List<User> getAllUsers() {
        return userRepo.findAll();
    }

    @Transactional(readOnly=true)
    public User authenticate(Credentials creds) {

        if (creds == null || creds.getUsername() == null || creds.getPassword() == null
            || creds.getUsername().equals("") || creds.getPassword().equals(""))
        {
            throw new BadRequestException("Invalid credentials object provided!");
        }

        User retrievedUser = userRepo.findUserByCredentials(creds);

        if (retrievedUser == null) {
            throw new AuthenticationException();
        }

        return retrievedUser;

    }

    @Transactional
    public int failcheck(Credentials creds) {

        if (creds == null || creds.getUsername() == null || creds.getPassword() == null
                || creds.getUsername().equals("") || creds.getPassword().equals(""))
        {
            throw new BadRequestException("Invalid credentials object provided!");
        }


        String us = creds.getUsername();
        String pw = creds.getPassword();
        int i = userRepo.failcheck(us,pw);

       return i;

    }

    @Transactional
    public User unblock(String username) {

        return userRepo.unblock(username);

    }

    @Transactional
    public void deleteUser(String username) {

         userRepo.deleteUser(username);

    }

    @Transactional
    public void register(User newUser) {

        // validation would go here...

        newUser.setRole(UserRole.BASIC_USER);
        newUser.setFailTime(0);
        userRepo.register(newUser);

    }

    @Transactional
    public User register1(User newUser) {

        // validation would go here...

        newUser.setRole(UserRole.BASIC_USER);
        newUser.setFailTime(0);
        return userRepo.save1(newUser);

    }

    @Transactional(readOnly=true)
    public User auth(Credentials creds) {

        if (creds == null || creds.getUsername() == null || creds.getPassword() == null
                || creds.getUsername().equals("") || creds.getPassword().equals(""))
        {
            throw new BadRequestException("Invalid credentials object provided!");
        }

        String us = creds.getUsername();
        String pw = creds.getPassword();
        User retrievedUser = userRepo.auth(us,pw);

        if (retrievedUser == null) {
            throw new AuthenticationException();
        }

        return retrievedUser;

    }

    @Transactional
    public void updatelist(FavoriteListCreds creds) {

        if (creds == null || creds.getMovieName() == null || creds.getApiId() == null || creds.getUserId() == 0
                || creds.getMovieName().equals("") || creds.getApiId().equals(""))
        {
            throw new BadRequestException("Invalid credentials object provided!");
        }


        FavoriteList l = new FavoriteList(creds.getMovieName(),creds.getApiId());

        userRepo.updateFavoriteList(l,creds.getUserId());


    }

    @Transactional
    public void updatelist1(FavoriteList l, int id) {


        userRepo.updateFavoriteList(l,id);


    }
}
