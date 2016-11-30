package sdfs.namenode;

import sdfs.filetree.DirNode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class DiskFlusher implements Runnable {
    private DirNode rootNode;
    private String fileTreePath;
    private long internalSeconds;
    private Logger logger;

    public DiskFlusher(DirNode rootNode, Logger logger, String fileTreePath, long internalSeconds) {
        this.rootNode = rootNode;
        this.logger = logger;
        this.fileTreePath = fileTreePath;
        this.internalSeconds = internalSeconds;
    }


    @Override
    public void run() {
        File file = new File(fileTreePath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
