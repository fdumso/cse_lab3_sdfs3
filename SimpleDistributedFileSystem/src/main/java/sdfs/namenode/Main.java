package sdfs.namenode;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        new NameNodeServer(10).run();
    }
}
