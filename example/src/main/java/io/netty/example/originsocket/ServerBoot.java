package io.netty.example.originsocket;

/**
 * @author Yao.Zhou
 * @since 2018/8/14 8:34
 */
public class ServerBoot {

    private static final int SERVER_PORT = 8000;

    public static void main(String[] args) {
        Server server = new Server(SERVER_PORT);
        server.start();
    }

}
