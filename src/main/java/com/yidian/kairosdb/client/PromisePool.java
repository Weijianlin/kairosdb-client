package com.yidian.kairosdb.client;

import io.netty.util.concurrent.Promise;
import io.netty.util.internal.ConcurrentSet;

/**
 * @author weijian
 * Date : 2016-05-31 16:59
 */

public class PromisePool {
    private final ConcurrentSet<Promise<Void>> pool;

    public PromisePool() {
        this.pool = new ConcurrentSet<>();
    }

    public void remove(Promise<Void> promise){
        pool.remove(promise);
    }

    public void add(Promise<Void> promise){
        pool.add(promise);
        // no matter success or not, remove the promise when done
        promise.addListener(f -> remove(promise));
    }

    public void awaitComplete(){
        for (Promise<Void> promise : pool) {
            promise.awaitUninterruptibly();
        }
    }
}
