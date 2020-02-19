package com.revature.movie;

import com.revature.movie.model.User;
import com.revature.movie.repos.UserRepos;
import com.revature.movie.services.UserService;
import com.revature.movie.web.dtos.Credentials;
import org.hibernate.SessionFactory;

public class Driver {

    private static SessionFactory sessionFactory;
    public static void main(String[] args) {

        Credentials creds = new Credentials();
        creds.setUsername("hle");
        creds.setPassword("pass");

        User u = new UserRepos(sessionFactory).findUserByCredentials(creds);
        System.out.println(u.toString());

    }

//    private static SessionFactory factory = HibernateUtil.getSessionFactory();
//    public static void main(String[] args) {
//        User[] users = {
//                new User("Hoang","le","hle","pass",0,"hoangle@email.com","admin"),
//                new User("admin","movie","admin1","pass",0,"admin1@email.com","admin"),
//                new User("member","movie","member","pass",0,"mem@email.com","member")
//        };
//
//
//
//
////        addUsers(users);
//
//        MovieLikes[] m = {
//                new MovieLikes("Superman",0),
//                new MovieLikes("Superman1",2),
//                new MovieLikes("Superman2",1),
//        };
//
////        addMovieLikes(m);
//
//        User[] users2 = {
//                new User("Jared","le","jleweweweew","pass12",0,"jle@ssemail.com","admin"),
//                new User("admindgsd","moviesuper","adminhjjhjh1424","pass424",0,"admin1ss441@email.com","admin"),
//                new User("memberbill","moviecool","member241fhfhfhhfhf4142","pass41413132",0,"mem142131@sssemail.com","member")
//        };
//
////      favoriteList(users2);
////
//        UserRepos repos = new UserRepos();
//        User u = new User();
//////       u =  repos.auth("hle","pass");
//////        System.out.println(u.toString());
////
////        User u1 = new User("Hoang","le","hle12","pass",0,"hoangle@email.com12","admin");
////        repos.register(u1);
//
////        FavoriteListRepos repos1 = new FavoriteListRepos();
////        FavoriteList l = repos1.findById(2);
////        System.out.println(l);
//
////        FavoriteList[] f = {
////                new FavoriteList("superman2","ii123"),
////                new FavoriteList("superman21","qwe123"),
////                new FavoriteList("superman2020","WW123"),
////
////        };
////
////        add123(f);
//
//
////       u= repos.findUserById(42);
////        System.out.println(u.getList());
////        for (FavoriteList f : u.getList())
////        System.out.println(f.getMovieName());
//
//        FavoriteList l = new FavoriteList("hello girl ","ii1432");
//
//        repos.updateFavoriteList(l,4);
//
//
//
//        List<FavoriteList> a = repos.findFavoriteByUserId(4);
//        for (FavoriteList f : a){
//            System.out.println(f.getMovieName());
//            System.out.println(f.getApiId());
//        }
//
//
//    }
//
//
//
//    public static void update (FavoriteList l){
//
//        Session session = factory.openSession();
//        session.beginTransaction();
//        User u = session.load(User.class, 4);
//        u.addLists(l);
//        session.update(u);
//        session.getTransaction().commit();
//        session.close();
//
//
//    }
//
//
//    public static void addUsers(User[] users) {
//
//        try (Session session = factory.getCurrentSession()) {
//
//            session.beginTransaction();
//
//
//
//            for (User s : users) session.save(s);
//            session.getTransaction().commit();
//
//            for (User s : users) System.out.println(s);
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    public static void addMovieLikes(MovieLikes[] m) {
//
//        try (Session session = factory.getCurrentSession()) {
//
//            session.beginTransaction();
//
//
//
//            for (MovieLikes s : m) session.save(s);
//            session.getTransaction().commit();
//
//            for (MovieLikes s : m) System.out.println(s);
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void favoriteList(User[] u) {
//
//        try (Session session = factory.getCurrentSession()) {
//
//            session.beginTransaction();
//
//            FavoriteList favoriteList1 = new FavoriteList("Superman");
//
//            for (User s : u){
//                favoriteList1.addUsers(s);
//
//            }
//
//            session.save(favoriteList1);
//            session.getTransaction().commit();
//
//
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//
//
//    public static void add123(FavoriteList[] a) {
//
//        try (Session session = factory.getCurrentSession()) {
//
//
//            session.beginTransaction();
//
//
//
//            User u = new User("Hoang","le","listcheck","pass",0,"hoangle@email.comchecls","admin");
//
//            for (FavoriteList s : a){
//                u.addLists(s);
//
//            }
//
//            session.save(u);
//            session.getTransaction().commit();
//
//
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
