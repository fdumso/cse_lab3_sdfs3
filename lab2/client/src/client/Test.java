package client;

import entity.SDFSFileChannel;
import protocol.INameNodeProtocol;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Test {
    public static void main(String[] args) {
        String fileUri = "/foo1/bar/test.pdf";
        String dirUri = "/foo/bar/te/";
        File testFile1 = new File("client/res/test1.pdf");
        File testFile2 = new File("client/res/test2.pdf");
        try {
            FileChannel fileChannel1 = new FileInputStream(testFile1).getChannel();
            int fileSize1 = (int)fileChannel1.size();
            ByteBuffer byteBuffer1 = ByteBuffer.allocate(fileSize1);
            fileChannel1.read(byteBuffer1);

            SimpleDistributedFileSystem sdfs = new SimpleDistributedFileSystem(new InetSocketAddress(InetAddress.getLocalHost(), INameNodeProtocol.NAME_NODE_PORT), 3);
            SimpleDistributedFileSystem sdfs2 = new SimpleDistributedFileSystem(new InetSocketAddress(InetAddress.getLocalHost(), INameNodeProtocol.NAME_NODE_PORT), 3);
            sdfs.mkdir(dirUri);

            SDFSFileChannel sdfsFileChannel1 = sdfs.create(fileUri);
            sdfsFileChannel1.write(byteBuffer1);
            sdfsFileChannel1.close();

            SDFSFileChannel sdfsFileChannel2 = sdfs2.openReadonly(fileUri);
            ByteBuffer byteBuffer2 = ByteBuffer.allocate((int) sdfsFileChannel2.size());
            sdfsFileChannel2.read(byteBuffer2);
            FileOutputStream fos1 = new FileOutputStream("client/res/test1_copy.pdf");
            fos1.write(byteBuffer2.array());
            fos1.flush();
            fos1.close();
            sdfsFileChannel2.close();

            FileChannel fileChannel2 = new FileInputStream(testFile2).getChannel();
            int fileSize2 = (int)fileChannel2.size();
            ByteBuffer byteBuffer3 = ByteBuffer.allocate(fileSize2);
            fileChannel2.read(byteBuffer3);
            SDFSFileChannel sdfsFileChannel3 = sdfs.openReadWrite(fileUri);
            sdfsFileChannel3.write(byteBuffer3);
            sdfsFileChannel3.close();

            SDFSFileChannel sdfsFileChannel4 = sdfs2.openReadonly(fileUri);
            ByteBuffer byteBuffer4 = ByteBuffer.allocate((int) sdfsFileChannel4.size());
            sdfsFileChannel4.read(byteBuffer4);
            FileOutputStream fos2 = new FileOutputStream("client/res/test2_copy.pdf");
            fos2.write(byteBuffer4.array());
            fos2.close();
            sdfsFileChannel4.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
