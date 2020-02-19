package com.revature.movie.web.controllers;

import com.revature.movie.model.User;
import com.revature.movie.model.UserRole;
import com.revature.movie.services.UserService;
import com.revature.movie.web.dtos.Credentials;
import com.revature.movie.web.dtos.UnblockCreds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/unblock")
public class UnBlock {


    private UserService userService;

    @Autowired
    public UnBlock(UserService service) {
        super();
        this.userService = service;
    }

    @PostMapping(produces= MediaType.APPLICATION_JSON_VALUE, consumes=MediaType.APPLICATION_JSON_VALUE)
    public User unblock(@RequestBody UnblockCreds username, HttpServletResponse response) {
        if (username.getRole() == UserRole.ADMIN){
            return userService.unblock(username.getUsername());
        }else {
            response.setStatus(HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION);//203
            return null;
        }


    }
}
