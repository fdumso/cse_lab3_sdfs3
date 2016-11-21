package sdfs.packet;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.filetree.LocatedBlock;
import sdfs.namenode.SDFSFileChannelData;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.channels.OverlappingFileLockException;
import java.util.List;

public class NameNodeResponse implements Serializable {
    private Type type;
    private SDFSFileChannelData sdfsFileChannelData;
    private List<LocatedBlock> blockList;

    private SDFSFileAlreadyExistsException sdfsFileAlreadyExistsException;
    private FileNotFoundException fileNotFoundException;
    private IllegalStateException illegalStateException;
    private IllegalArgumentException illegalArgumentException;
    private IllegalAccessTokenException illegalAccessTokenException;
    private OverlappingFileLockException overlappingFileLockException;
    private IndexOutOfBoundsException indexOutOfBoundsException;


    public NameNodeResponse(Type type) {
        this.type = type;
    }

    public enum Type {
        OPEN_READ_ONLY, OPEN_READ_WRITE, CREATE, MK_DIR, CLOSE_READ_ONLY, CLOSE_READ_WRITE, ADD_BLOCKS, REMOVE_LAST_BLOCKS, NEW_COW_BLOCK
    }

    public Type getType() {
        return type;
    }

    public SDFSFileAlreadyExistsException getSDFSFileAlreadyExistsException() {
        return sdfsFileAlreadyExistsException;
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

    public SDFSFileChannelData getSDFSFileChannelData() {
        return sdfsFileChannelData;
    }

    public List<LocatedBlock> getBlockList() {
        return blockList;
    }

    public IllegalAccessTokenException getIllegalAccessTokenException() {
        return illegalAccessTokenException;
    }

    public OverlappingFileLockException getOverlappingFileLockException() {
        return overlappingFileLockException;
    }

    public IndexOutOfBoundsException getIndexOutOfBoundsException() {
        return indexOutOfBoundsException;
    }

    public void setSDFSFileChannelData(SDFSFileChannelData sdfsFileChannelData) {
        this.sdfsFileChannelData = sdfsFileChannelData;
    }

    public void setBlockList(List<LocatedBlock> blockList) {
        this.blockList = blockList;
    }

    public void setSDFSFileAlreadyExistException(SDFSFileAlreadyExistsException sdfsFileAlreadyExistsException) {
        this.sdfsFileAlreadyExistsException = sdfsFileAlreadyExistsException;
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

    public void setIllegalAccessTokenException(IllegalAccessTokenException illegalAccessTokenException) {
        this.illegalAccessTokenException = illegalAccessTokenException;
    }

    public void setOverlappingFileLockException(OverlappingFileLockException overlappingFileLockException) {
        this.overlappingFileLockException = overlappingFileLockException;
    }

    public void setIndexOutOfBoundsException(IndexOutOfBoundsException indexOutOfBoundsException) {
        this.indexOutOfBoundsException = indexOutOfBoundsException;
    }
}
