package com.revature.movie.repos;

import com.revature.movie.model.FavoriteList;
import com.revature.movie.model.User;
import com.revature.movie.web.dtos.Credentials;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
public class UserRepos implements CrudRepository {


    private SessionFactory sessionFactory;




    public User findUserByCredentials(Credentials creds) {

        Session session = sessionFactory.getCurrentSession();
        User u = new User();
        u = session.createQuery("from User where username = :un and password = :pw", User.class)
                .setParameter("un", creds.getUsername())
                .setParameter("pw", creds.getPassword())
                .getSingleResult();
        return u;

    }

    @Autowired
    public UserRepos(SessionFactory factory) {
        super();
        this.sessionFactory = factory;
    }


    public void updateFavoriteList (FavoriteList l, int id){

        Session session = sessionFactory.getCurrentSession();
        User u = session.get(User.class, id);
        u.addLists(l);
        session.update(u);


    }

    public User updateFavoriteList1 (FavoriteList l, int id){

        FavoriteListRepos a = new FavoriteListRepos(sessionFactory);
        a.add(l);
        Session session = sessionFactory.getCurrentSession();

        User u = session.createQuery("insert into User_favorite (UserId, favoriteId) values (:uid, :fid)", User.class)
                .setParameter("uid", id)
                .setParameter("fid", l.getFavoriteId())
                .getSingleResult();

        return u;


    }


    public List<FavoriteList> findFavoriteByUserId(int id){
        List<FavoriteList> fl = new ArrayList<>();
        User u = findUserById(id);

        for(FavoriteList l : u.getList()){
            fl.add(l);
        }



        return  fl;
    }

    public User auth(String username, String password) {

        User u = new User();

        try {

            Session session = sessionFactory.getCurrentSession();
            CriteriaBuilder queryBuilder = session.getCriteriaBuilder();


            CriteriaQuery<User> critQuery = queryBuilder.createQuery(User.class);


            Root<User> queryRoot = critQuery.from(User.class);


            critQuery.select(queryRoot);


            critQuery.where(
                    queryBuilder.equal(queryRoot.get("username"), username),
                    queryBuilder.equal(queryRoot.get("password"), password)
            );

            List<User> userList = session.createQuery(critQuery).getResultList();
            u = userList.get(0);
            return u;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return u;
    }


    public void deleteUser(String username) {

        User u = new User();

        try {

            Session session = sessionFactory.getCurrentSession();
            CriteriaBuilder queryBuilder = session.getCriteriaBuilder();


            CriteriaQuery<User> critQuery = queryBuilder.createQuery(User.class);


            Root<User> queryRoot = critQuery.from(User.class);


            critQuery.select(queryRoot);


            critQuery.where(
                    queryBuilder.equal(queryRoot.get("username"), username)
            );

            List<User> userList = session.createQuery(critQuery).getResultList();
            u = userList.get(0);
            session.delete(u);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public User unblock(String username) {

        User u = new User();

        try {

            Session session = sessionFactory.getCurrentSession();
            CriteriaBuilder queryBuilder = session.getCriteriaBuilder();


            CriteriaQuery<User> critQuery = queryBuilder.createQuery(User.class);


            Root<User> queryRoot = critQuery.from(User.class);


            critQuery.select(queryRoot);


            critQuery.where(
                    queryBuilder.equal(queryRoot.get("username"), username)
            );

            List<User> userList = session.createQuery(critQuery).getResultList();
            u = userList.get(0);
            u.setFailTime(0);
            session.update(u);
            return u;


        } catch (Exception e) {
            e.printStackTrace();
        }
        return u;
    }

    public int failcheck(String username, String password) {

        int checkfail = 0 ;
        User u = new User();

        try {

            Session session = sessionFactory.getCurrentSession();
            CriteriaBuilder queryBuilder = session.getCriteriaBuilder();


            CriteriaQuery<User> critQuery = queryBuilder.createQuery(User.class);


            Root<User> queryRoot = critQuery.from(User.class);


            critQuery.select(queryRoot);


            critQuery.where(
                    queryBuilder.equal(queryRoot.get("username"), username)
            );

            List<User> userList = session.createQuery(critQuery).getResultList();
            u = userList.get(0);
            if(u.getFailTime() < 5){
                if (u.getPassword().equals(password)){
                    u.setFailTime(0);
                    session.update(u);
                    return 1;
                }else {
                    checkfail = u.getFailTime() + 1;
                    u.setFailTime(checkfail);
                    session.update(u);
                    return 0;
                }
            }else {
                return 3;//lock
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


    public void register(User u){
        Session session = sessionFactory.getCurrentSession();
        session.save(u);

    }

    public User save1(User newObj) {
        Session session = sessionFactory.getCurrentSession();
        session.save(newObj);
        return newObj;
    }

    public User findUserById(int id){

        User l = new User();
        Session session = sessionFactory.getCurrentSession();
        l = session.get(User.class, id);

        return l;

    }


    @Override
    public void save(Object newObj) {

    }

    @Override
    public List<User> findAll() {
        Session session = sessionFactory.getCurrentSession();
        return session.createQuery("from AppUser", User.class).getResultList();
    }

    @Override
    public Optional findById(int id) {
        return Optional.empty();
    }

    @Override
    public boolean update(Object updatedObj) {
        return false;
    }

    @Override
    public boolean deleteById(int id) {
        return false;
    }
}
