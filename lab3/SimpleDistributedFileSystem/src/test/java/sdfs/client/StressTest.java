package sdfs.client;

import sdfs.datanode.DataNode;
import sdfs.datanode.DataNodeServer;
import sdfs.namenode.NameNodeServer;
import sdfs.protocol.SDFSConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static sdfs.Util.generatePort;

public class StressTest {
    private static String base62(int x) {
        StringBuilder sb = new StringBuilder();
        ++x;
        while (x > 0) {
            int r = x % 62;
            x = x / 62;
            if (r < 26) {
                sb.append('A' + r);
            } else if (r < 52) {
                sb.append('a' + (r - 26));
            } else {
                sb.append('0' + (r - 52));
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setProperty("sdfs.namenode.dir", "namenode");
        System.setProperty("sdfs.datanode.dir", "datanode");
        SDFSConfiguration configuration = new SDFSConfiguration(InetAddress.getLocalHost(), generatePort(), InetAddress.getLocalHost(), generatePort());
        NameNodeServer nameNodeServer = new NameNodeServer(configuration, 10);
        DataNodeServer dataNodeServer = new DataNodeServer(configuration);
        new Thread(nameNodeServer).start();
        new Thread(dataNodeServer).start();

        SDFSClient client = new SDFSClient(configuration, 3);

        int nthreads = 16, nfiles = 64;
        int maxBlocks = 8;
        AtomicInteger requests = new AtomicInteger(0);
        Thread[] threads = new Thread[nthreads];

        long time1 = System.currentTimeMillis();

        for (int i = 0; i<nthreads; i++) {
            String dirName = base62(i);
            client.mkdir(dirName);
            threads[i] = new Thread(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                int localRequests = 0;
                ByteBuffer bb = ByteBuffer.allocate(1);
                bb.put((byte) 1);
                for (int j = 0; j<nfiles; j++) {
                    try {
                        String fileName = dirName + "/" + base62(j);
                        SDFSFileChannel fc = client.create(fileName);
                        int blocks = random.nextInt(maxBlocks);
                        for (int k = 0; k<blocks; k++) {
                            fc.position(k * DataNode.BLOCK_SIZE);
                            bb.position(0);
                            fc.write(bb);
                        }
                        fc.close();
                        localRequests += blocks + 2;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                requests.addAndGet(localRequests);
            });
            threads[i].start();
        }

        for (Thread thread: threads) {
            thread.join();
        }

        long time2 = System.currentTimeMillis();
        System.err.printf("%d requests in %dms", requests.get(), (time2 - time1));
    }
}
