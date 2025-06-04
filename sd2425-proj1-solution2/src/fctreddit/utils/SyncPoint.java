package fctreddit.utils;

import fctreddit.api.java.Result;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncPoint {

    private final Map<Long, Result<?>> result;
    private long version;

    private SyncPoint() {
        this.result = new ConcurrentHashMap<>();
        this.version = -1;
    }

    private static SyncPoint instance = null;

    public static SyncPoint getSyncPoint() {
        if(SyncPoint.instance == null)
            SyncPoint.instance = new SyncPoint();

        return SyncPoint.instance;
    }

    public synchronized Result<?> waitForResult(long n ) {
        while( version < n ) {
            try {
                wait();
            } catch (InterruptedException e) {
                // nothing to be done here
            }
        }

        return result.remove(n);
    }

    public synchronized void waitForVersion( long n ) {
        while( version < n ) {
            try {
                wait();
            } catch (InterruptedException e) {
                // nothing to be done here
            }
        }
    }

    public synchronized void setResult( long n, Result<?> res ) {
        if ( res != null ) {
            result.put(n, res);
        }
        version = n;
        notifyAll();
    }

    public synchronized long getVersion() {
        return version;
    }

}
