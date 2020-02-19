package com.revature.movie.web.controllers;

import com.revature.movie.model.FavoriteList;
import com.revature.movie.model.User;
import com.revature.movie.repos.FavoriteListRepos;
import com.revature.movie.services.Flservice;
import com.revature.movie.services.UserService;
import com.revature.movie.web.dtos.Credentials;
import com.revature.movie.web.dtos.FavoriteListCreds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/addlist")
public class FavoriteListController {
    private UserService userService;
    private Flservice fl;

    @Autowired
    public FavoriteListController(UserService service, Flservice fl) {
        super();
        this.userService = service;
        this.fl = fl;
    }

    @PostMapping(produces= MediaType.APPLICATION_JSON_VALUE, consumes=MediaType.APPLICATION_JSON_VALUE)
    public void authenticate(@RequestBody FavoriteListCreds creds) {

        FavoriteList a = new FavoriteList(creds.getMovieName(),creds.getApiId());
        userService.updatelist1(fl.register(a),creds.getUserId());





    }
}
