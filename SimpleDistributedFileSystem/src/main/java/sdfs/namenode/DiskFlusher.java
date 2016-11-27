package sdfs.namenode;

import sdfs.filetree.DirNode;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class DiskFlusher implements Runnable {
    private DirNode rootNode;
    private String logFilePath;
    private long internalSeconds;

    public DiskFlusher(DirNode rootNode, String logFilePath, long internalSeconds) {
        this.rootNode = rootNode;
        this.logFilePath = logFilePath;
        this.internalSeconds = internalSeconds;
    }


    @Override
    public void run() {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(logFilePath);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(rootNode);
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
