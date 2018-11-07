package sdfs.namenode;

import sdfs.filetree.DirNode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.locks.ReentrantLock;

public class DiskFlusher implements Runnable {
    private DirNode rootNode;
    private long internalSeconds;
    private Logger logger;
    private File file;

    DiskFlusher(DirNode rootNode, Logger logger, String fileTreePath, long internalSeconds) {
        this.rootNode = rootNode;
        this.logger = logger;
        this.internalSeconds = internalSeconds;
        file = new File(fileTreePath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void run() {

        try {
            while (true) {
                FileOutputStream fileOutputStream = new FileOutputStream(file, false);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
                logger.prepToFlush();
                objectOutputStream.writeObject(rootNode);
                objectOutputStream.flush();
                objectOutputStream.close();
                logger.checkPoint();
                Thread.sleep(1000 * internalSeconds);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
