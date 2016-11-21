package sdfs.protocol;

import sdfs.namenode.AccessTokenPermission;

import java.rmi.Remote;
import java.util.Set;
import java.util.UUID;

public interface INameNodeDataNodeProtocol extends Remote {
    /**
     * Get original file access token permission
     *
     * @return Access token permission
     */
    AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken);

    /**
     * Get new allocated block of this file
     *
     * @return New allocated block of this file
     */
    Set<Integer> getAccessTokenNewBlocks(UUID fileAccessToken);

}
