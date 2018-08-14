package io.netty.example.originsocket;

import java.io.IOException;
import java.net.Socket;

/**
 * @author Yao.Zhou
 * @since 2018/8/14 8:28
 */
public class Client {

    private static final String SERVER_HOST = "127.0.0.1";
    private static final int SERVER_PORT = 8000;
    private static final int SLEEP_TIME = 5000;

    public static void main(String[] args) throws IOException {
        final Socket socket = new Socket(SERVER_HOST, SERVER_PORT);

        new Thread(() -> {
            System.out.print("Client start success...");

            while (true) {
                String message = "msg=Hello World&timestamp="+ System.currentTimeMillis();
                System.out.println("Client send message: " + message);

                try {
                    socket.getOutputStream().write(message.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }

                sleep();
            }
        }).start();
    }

    private static void sleep() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
