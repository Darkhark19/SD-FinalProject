package util;

import java.util.concurrent.atomic.AtomicLong;

public class Version {

    private static Long version;

    public Long get() {
        return version;
    }

    public static void set(Long v) {
        version = v;
    }

    public void inc() {
        version++;
    }
}
