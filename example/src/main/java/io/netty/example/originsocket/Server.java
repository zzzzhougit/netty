package io.netty.example.originsocket;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author Yao.Zhou
 * @since 2018/8/14 8:17
 */
public class Server {

    private ServerSocket serverSocket;

    public Server(int port) {
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void doStart() {
        while (true) {
            try {
                Socket client = serverSocket.accept();
                new ClientHandler(client).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void start() {
        new Thread(() -> {
            doStart();
        }).start();
    }

}
