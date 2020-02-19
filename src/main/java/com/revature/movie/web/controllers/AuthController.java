package com.revature.movie.web.controllers;


import com.revature.movie.services.UserService;
import com.revature.movie.web.dtos.Credentials;
import com.revature.movie.web.dtos.Principal;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/auth")
public class AuthController {

    private UserService userService;

    @Autowired
    public AuthController(UserService service) {
        super();
        this.userService = service;
    }

    @PostMapping(produces=MediaType.APPLICATION_JSON_VALUE, consumes=MediaType.APPLICATION_JSON_VALUE)
    public Principal authenticate(@RequestBody Credentials creds) {
        return userService.authenticate(creds).extractPrincipal();
    }
}
