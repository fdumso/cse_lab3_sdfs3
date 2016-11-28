package sdfs.packet;

import sdfs.entity.AccessTokenPermission;
import sdfs.entity.SDFSFileChannelData;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistsException;
import sdfs.filetree.LocatedBlock;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.channels.OverlappingFileLockException;
import java.util.List;

public class NameNodeResponse implements Serializable {
    private SDFSFileChannelData sdfsFileChannelData;
    private List<LocatedBlock> blockList;
    private AccessTokenPermission accessTokenPermission;

    private SDFSFileAlreadyExistsException sdfsFileAlreadyExistsException;
    private FileNotFoundException fileNotFoundException;
    private IllegalArgumentException illegalArgumentException;
    private IllegalAccessTokenException illegalAccessTokenException;
    private OverlappingFileLockException overlappingFileLockException;
    private IndexOutOfBoundsException indexOutOfBoundsException;

    public SDFSFileAlreadyExistsException getSDFSFileAlreadyExistsException() {
        return sdfsFileAlreadyExistsException;
    }

    public FileNotFoundException getFileNotFoundException() {
        return fileNotFoundException;
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

    public AccessTokenPermission getAccessTokenPermission() {
        return accessTokenPermission;
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

    public void setAccessTokenPermission(AccessTokenPermission accessTokenPermission) {
        this.accessTokenPermission = accessTokenPermission;
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
