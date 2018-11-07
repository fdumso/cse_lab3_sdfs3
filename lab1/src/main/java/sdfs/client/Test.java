package sdfs.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Test {
//    private static String helpInfo = "Simple Distributed File System.\n\n" +
//            "Usage: java -jar sdfs.jar [OPTION]... [URL]\n\n" +
//            "no option\tConnect to NameNode specified by URL\n" +
//            "-h,\t--help\tThis help information\n";
    public static void main(String[] args) {
//        String url = "sdfs://127.0.0.1:7000/";
//        try {
//            for (int i = 0; i < args.length; i++) {
//                switch (args[i]) {
//                    case "-h":
//                    case "--help": {
//                        System.out.println(helpInfo);
//                        break;
//                    }
//                    default: {
//                        url = args[i];
//                    }
//                }
//            }
//        }
//        catch (Exception e) {
//            System.err.println("Invalid argument!");
//        }

        SimpleDistributedFileSystem sdfs = new SimpleDistributedFileSystem();
        try {
//            String fileUri = "/foo";
//            System.out.println(fileUri.substring(fileUri.lastIndexOf('/')+1));
//            System.out.println(fileUri.split("/")[1]);
//            System.out.println(fileUri.split("/")[2]);

            // load a file in client
            FileInputStream fis = new FileInputStream("test.pdf");
            byte[] testFileBytes = new byte[512*1024];
            int size = fis.read(testFileBytes);
            // make some directories in sdfs
            sdfs.mkdir("/foo");
            sdfs.mkdir("/foo/bar");
            // write the file into sdfs
            SDFSOutputStream os = sdfs.create("/foo/bar/test.pdf");
            os.write(testFileBytes);
            os.close();
            // read the file
            SDFSInputStream is = sdfs.open("/foo/bar/test.pdf");
            byte[] bytes = new byte[512*1024];
            is.read(bytes);
            is.close();
            // write the file into client in a different name
            FileOutputStream fos = new FileOutputStream("test_copy.pdf");
            fos.write(bytes, 0, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
