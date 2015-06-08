package com.elasticm2m.frameworks.http;

import backtype.storm.utils.Utils;
import com.github.nkzawa.socketio.client.IO;
import com.github.nkzawa.socketio.client.Socket;
import org.junit.Ignore;
import org.junit.Test;

public class SocketIoTest {

    private String endpoint = "core://localhost:3000";

    @Test
    @Ignore
    public void testSocket() throws Exception {
        Socket socket = IO.socket(endpoint);

        socket.on("chat message", (args) -> {
            System.out.println("args:" + args[0]);
        });

        socket.connect();

        while (true) {
            Utils.sleep(1000);
        }
    }
}
