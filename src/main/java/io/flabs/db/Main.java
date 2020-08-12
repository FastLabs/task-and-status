package io.flabs.db;


import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;

public class Main {

    public static void main (String ... args) {
        final String driver = "org.apache.derby.jdbc.EmbeddedDriver";

        Future<Message<String>> x = Future.future();
        Future<String> y = x.map(Message::body);
        try {
            Class.forName(driver).newInstance();


        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
