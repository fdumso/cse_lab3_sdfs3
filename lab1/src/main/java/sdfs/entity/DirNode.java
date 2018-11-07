/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.entity;

import sdfs.namenode.Entry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DirNode {
    private List<Entry> entryList;


    public DirNode() {
        this.entryList = new ArrayList<>();
    }

    public Entry findChild(String name) {
        for (Entry entry:
             entryList) {
            if (entry.getName().equals(name)) {
                return entry;
            }
        }
        return null;
    }

    public void addEntry(Entry entry) {
        entryList.add(entry);
    }

    public byte[] toBytes() {
        int size = 0;
        for (Entry entry: entryList) {
            size += 9+entry.getName().getBytes().length;
        }
        byte[] result = new byte[size];
        ByteBuffer resultBuffer = ByteBuffer.wrap(result);
        for (Entry entry :
                entryList) {
            byte[] nameBytes = entry.getName().getBytes();
            int nameLength = nameBytes.length;
            byte signByte;
            if (entry.getType() == Entry.NodeType.FILE) {
                signByte = 0;
            } else {
                signByte = 1;
            }
            resultBuffer.put(signByte);
            resultBuffer.putInt(nameLength);
            resultBuffer.put(nameBytes);
            resultBuffer.putInt(entry.getId());
        }

        return result;
    }


}

