package sdfs.namenode;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            new NameNodeServer(10).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
