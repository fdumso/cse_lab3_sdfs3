package response;

import entity.LocatedBlock;
import entity.SDFSFileChannel;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

public class NameNodeResponse implements Serializable {
    private Type type;
    private SDFSFileChannel sdfsFileChannel;
    private List<LocatedBlock> blockList;

    private FileAlreadyExistsException fileAlreadyExistsException;
    private FileNotFoundException fileNotFoundException;
    private IllegalStateException illegalStateException;
    private IllegalArgumentException illegalArgumentException;


    public NameNodeResponse(Type type) {
        this.type = type;
    }

    public enum Type {
        OPEN_READ_ONLY, OPEN_READ_WRITE, CREATE, MK_DIR, CLOSE_READ_ONLY, CLOSE_READ_WRITE, ADD_BLOCKS, REMOVE_LAST_BLOCKS
    }

    public Type getType() {
        return type;
    }

    public FileAlreadyExistsException getFileAlreadyExistsException() {
        return fileAlreadyExistsException;
    }

    public FileNotFoundException getFileNotFoundException() {
        return fileNotFoundException;
    }

    public IllegalStateException getIllegalStateException() {
        return illegalStateException;
    }

    public IllegalArgumentException getIllegalArgumentException() {
        return illegalArgumentException;
    }

    public SDFSFileChannel getSdfsFileChannel() {
        return sdfsFileChannel;
    }

    public List<LocatedBlock> getBlockList() {
        return blockList;
    }


    public void setSdfsFileChannel(SDFSFileChannel sdfsFileChannel) {
        this.sdfsFileChannel = sdfsFileChannel;
    }

    public void setBlockList(List<LocatedBlock> blockList) {
        this.blockList = blockList;
    }

    public void setFileAlreadyExistsException(FileAlreadyExistsException fileAlreadyExistsException) {
        this.fileAlreadyExistsException = fileAlreadyExistsException;
    }

    public void setFileNotFoundException(FileNotFoundException fileNotFoundException) {
        this.fileNotFoundException = fileNotFoundException;
    }

    public void setIllegalStateException(IllegalStateException illegalStateException) {
        this.illegalStateException = illegalStateException;
    }

    public void setIllegalArgumentException(IllegalArgumentException illegalArgumentException) {
        this.illegalArgumentException = illegalArgumentException;
    }
}
