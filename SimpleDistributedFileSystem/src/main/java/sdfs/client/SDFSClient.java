package sdfs.client;

import java.io.IOException;

public class SDFSClient implements ISDFSClient {
    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        return null;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        return null;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        return null;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {

    }
}
