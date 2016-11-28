package sdfs.client;

import sdfs.exception.SDFSFileAlreadyExistsException;

import java.io.FileNotFoundException;

public interface ISDFSClient {
    /**
     * Open a readonly file that is already exist.
     *
     * @param fileUri The file uri to be open. The fileUri should look like /foo/bar.data which is a request to sdfs://[ip]:[port]/foo/bar.data
     * @return FileInfo channel of this file
     * @throws FileNotFoundException if the file is not exist
     */
    SDFSFileChannel openReadonly(String fileUri) throws FileNotFoundException;

    /**
     * Open a read write file that is already exist.
     *
     * @param fileUri The file uri to be create. The fileUri should look like /foo/bar.data which is a request to sdfs://[ip]:[port]/foo/bar.data
     * @return file channel of this file
     * @throws FileNotFoundException if the file is not exist
     */
    SDFSFileChannel openReadWrite(String fileUri) throws FileNotFoundException;

    /**
     * Create a empty file and return the output stream to this file.
     *
     * @param fileUri The file uri to be create. The fileUri should look like /foo/bar.data which is a request to sdfs://[ip]:[port]/foo/bar.data
     * @return FileInfo channel of this file
     * @throws SDFSFileAlreadyExistsException if the file is already exist
     */
    SDFSFileChannel create(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException;

    /**
     * Make a directory on given file uri.
     *
     * @param fileUri the directory path
     * @throws SDFSFileAlreadyExistsException if directory or file is already exist
     */
    void mkdir(String fileUri) throws SDFSFileAlreadyExistsException, FileNotFoundException;
}
