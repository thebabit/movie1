package com.revature.movie.services;

import com.revature.movie.model.FavoriteList;
import com.revature.movie.model.User;
import com.revature.movie.model.UserRole;
import com.revature.movie.repos.FavoriteListRepos;
import com.revature.movie.repos.UserRepos;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class Flservice {
    private FavoriteListRepos repo;

    @Autowired
    public Flservice(FavoriteListRepos repo) {
        super();
        this.repo = repo;
    }

    @Transactional
    public FavoriteList register(FavoriteList l) {

        // validation would go here...

       return repo.add(l);

    }
}
