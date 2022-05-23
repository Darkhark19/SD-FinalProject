package tp1.impl.clients;

import tp1.api.User;
import tp1.api.service.java.Users;

public class Client {
    public static void main(String[] args) throws Exception {

        Users us = Clients.UsersClients.get();

        us.createUser( new User("smd", "Sérgio Duarte", "smd@fct.unl.pt", "12345"));
        us.createUser( new User("nmp", "Nuno Preguiça", "nmp@fct.unl.pt", "54321"));

        us.searchUsers("").value().forEach( System.out::println );

    }
}
