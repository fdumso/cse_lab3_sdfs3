package sdfs.protocol;

import sdfs.entity.AccessTokenPermission;

import java.net.InetAddress;
import java.util.UUID;

public interface INameNodeDataNodeProtocol {
    /**
     * Get current file access token permission
     *
     * @return Access token permission
     */
    AccessTokenPermission getAccessTokenPermission(UUID fileAccessToken, InetAddress dataNodeAddress);

}
