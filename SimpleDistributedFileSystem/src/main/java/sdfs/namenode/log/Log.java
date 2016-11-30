package sdfs.namenode.log;

import java.io.Serializable;

public abstract class Log implements Serializable {
    private int id;
    private Type type;

    public enum Type {
        START, ABORT, ADD_BLOCKS, CHECK_POINT, CLOSE_READ, CLOSE_WRITE, COMMIT, COPY_ON_WRITE_BLOCK,
        CREATE, MK_DIR, OPEN_READ, OPEN_WRITE, REMOVE_BLOCKS;

        @Override
        public String toString() {
            switch (this) {
                case MK_DIR: return "mkdir";
                case COMMIT: return "commit";
                case START: return "start";
                case ABORT: return "abort";
                case COPY_ON_WRITE_BLOCK: return "copy_on_write_block";
                case ADD_BLOCKS: return "add_block";
                case CHECK_POINT: return "check_point";
                case CLOSE_READ: return "close_read";
                case CLOSE_WRITE: return "close_write";
                case CREATE: return "create";
                case OPEN_READ: return "open_read";
                case OPEN_WRITE: return "open_write";
                case REMOVE_BLOCKS: return "remove_blocks";
                default: return "";
            }
        }
    }

    public Log(int id, Type type) {
        this.id = id;
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public Type getType() {
        return type;
    }

}
