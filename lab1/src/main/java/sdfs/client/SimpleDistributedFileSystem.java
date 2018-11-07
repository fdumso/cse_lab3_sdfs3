package sdfs.client;

import sdfs.entity.FileNode;
import sdfs.namenode.NameNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

public class SimpleDistributedFileSystem implements ISimpleDistributedFileSystem {
    private NameNode nameNode = NameNode.getInstance();

//    private InetAddress namenodeAddress;
//    private int namenodePort;
//
//    private static final String IP_PATTERN =
//            "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
//                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
//                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
//                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5]))";
//    private static final String PORT_PATTERN = "(\\d+)";

    public SimpleDistributedFileSystem() {
//        // parse the url
//        Pattern urlPattern = Pattern.compile("sdfs://"+IP_PATTERN+":"+PORT_PATTERN+"[/]?");
//        Matcher matcher = urlPattern.matcher(url);
//        if (!matcher.matches()) {
//            System.err.println("URL invalid!");
//            throw new UnknownHostException();
//        }
//        String ipstr = matcher.group(1);
//        String portstr = matcher.group(6);
//        namenodeAddress = InetAddress.getByAddress(ipstr.getBytes());
//        namenodePort = Integer.parseInt(portstr);

    }

    @Override
    public SDFSInputStream open(String fileUri) throws FileNotFoundException, IOException {
        FileNode fileNode = nameNode.open(fileUri);
        SDFSInputStream is = null;
        if (fileNode != null) {
            is = new SDFSInputStream(fileNode, fileUri);
        }
        return is;
    }

    @Override
    public SDFSOutputStream create(String fileUri) throws FileAlreadyExistsException, IOException {
        FileNode fileNode = nameNode.create(fileUri);
        SDFSOutputStream os = null;
        if (fileNode != null) {
            os = new SDFSOutputStream(fileNode, fileUri);
        }
        return os;
    }

    @Override
    public void mkdir(String fileUri) throws FileAlreadyExistsException, IOException {
        nameNode.mkdir(fileUri);
    }
}
