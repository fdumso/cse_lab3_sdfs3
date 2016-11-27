package sdfs.filetree;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class DirNode extends Node implements Serializable, Iterable<Entry> {
    private static final long serialVersionUID = 8178778592344231767L;
    private final AtomicReference<HashSet<Entry>> entries = new AtomicReference<>();

    public DirNode() {
        super(Type.DIR);
        entries.set(new HashSet<>());
    }

    public Iterator<Entry> iterator() {
        return entries.get().iterator();
    }

    public Entry findEntry(String name) {
        for (Entry e:
             entries.get()) {
            if (e.getName().equals(name)) {
                return e;
            }
        }
        return null;
    }

    public DirNode addEntry(Entry entry) {
        entries.updateAndGet(entries -> {
            entries.add(entry);
            return entries;
        });
        return this;
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
