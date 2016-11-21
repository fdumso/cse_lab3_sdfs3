package sdfs.filetree;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirNode extends Node implements Serializable, Iterable<Entry> {
    private static final long serialVersionUID = 8178778592344231767L;
    private final Set<Entry> entries = new HashSet<>();

    public DirNode() {
        super(Type.DIR);
    }

    @Override
    public Iterator<Entry> iterator() {
        return entries.iterator();
    }

    public Entry findEntry(String name) {
        for (Entry e:
             entries) {
            if (e.getName().equals(name)) {
                return e;
            }
        }
        return null;
    }

    public boolean addEntry(Entry entry) {
        return entries.add(entry);
    }

    public boolean removeEntry(Entry entry) {
        return entries.remove(entry);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DirNode entries1 = (DirNode) o;

        return entries.equals(entries1.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }
}
