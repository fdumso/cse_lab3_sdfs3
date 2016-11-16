package sdfs.datanode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeDataNodeProtocol;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class DataNodeServer implements IDataNodeProtocol {
    private INameNodeDataNodeProtocol data2name;

    public DataNodeServer(INameNodeDataNodeProtocol data2name) {
        this.data2name = data2name;
    }

    @Override
    public byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        return new byte[0];
    }

    @Override
    public void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException, IOException {

    }
}
