package sdfs.namenode;

import sdfs.filetree.FileNode;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class OpeningFileManager {
    // map currently opened read-only file token to its file node
    private final ConcurrentMap<UUID, FileNode> reading = new ConcurrentHashMap<>();

    // map currently opened read-write file token to its accordingly writing file
    private final ConcurrentMap<UUID, WritingFile> writing = new ConcurrentHashMap<>();


    boolean isReading(UUID token) {
        return reading.containsKey(token);
    }

    boolean isWriting(UUID token) {
        return writing.containsKey(token);
    }

    FileNode getReadingFile(UUID token) {
        return reading.get(token);
    }

    FileNode getWritingFile(UUID token) {
        return writing.get(token).currentFile;
    }

    FileNode openRead(UUID token, FileNode fileNode) {
        // copy the file node to keep the data unchanged
        FileNode readingNode = fileNode.copy();
        reading.putIfAbsent(token, readingNode);
        return readingNode;
    }

    FileNode openWrite(UUID token, FileNode fileNode) {

    }

    class WritingFile {
        private FileNode originalFile;
        private FileNode currentFile;

        public WritingFile(FileNode originalFile) {
            this.originalFile = originalFile;
            this.currentFile = originalFile.copy();
        }
    }
}
