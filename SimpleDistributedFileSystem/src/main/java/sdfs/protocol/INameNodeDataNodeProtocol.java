package sdfs.protocol;

import sdfs.namenode.AccessTokenPermission;

import java.rmi.Remote;
import java.util.Set;
import java.util.UUID;

public interface INameNodeDataNodeProtocol {
    /**
     * Get current file access token permission
     *
     * @return Access token permission
     */
    AccessTokenPermission getAccessTokenPermission(UUID fileAccessToken);

}
