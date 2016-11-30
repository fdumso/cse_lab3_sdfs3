package sdfs.filetree;

import java.io.Serializable;

public class Entry implements Serializable {
    private final Node node;
    private String name;

    Entry(String name, Node node) {
        if (name == null || node == null) {
            throw new NullPointerException();
        }
        if (name.isEmpty() || name.contains("/")) {
            throw new IllegalArgumentException();
        }
        this.name = name;
        this.node = node;
    }

    String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entry entry = (Entry) o;

        return name.equals(entry.name);

    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public Node getNode() {
        return node;
    }
}
