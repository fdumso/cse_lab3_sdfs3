/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;

    @Override
    public int read(ByteBuffer dst) throws IOException {
        //todo your code here
        return 0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        //todo your code here
        return 0;
    }

    @Override
    public long position() throws IOException {
        //todo your code here
        return 0;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        //todo your code here
        return null;
    }

    @Override
    public long size() throws IOException {
        //todo your code here
        return 0;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        //todo your code here
        return null;
    }

    @Override
    public boolean isOpen() {
        //todo your code here
        return false;
    }

    @Override
    public void close() throws IOException {
        //todo your code here
    }

    @Override
    public void flush() throws IOException {
        //todo your code here
    }
}
