/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.entity;

import java.io.Serializable;
import java.net.InetAddress;

public class LocatedBlock implements Serializable {
    private static final long serialVersionUID = -6509598325324530684L;
    private final InetAddress inetAddress;
    private final int blockNumber;
    private int size;

    public LocatedBlock(InetAddress inetAddress, int blockNumber) {
        this.inetAddress = inetAddress;
        this.blockNumber = blockNumber;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
