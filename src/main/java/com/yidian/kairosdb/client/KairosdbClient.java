package com.yidian.kairosdb.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author weijian
 * Date : 2015-12-14 20:53
 */

public class KairosdbClient {
    private static final Logger LOG = LoggerFactory.getLogger(KairosdbClient.class);

    private final FixedChannelPool channelPool;
    private final NioEventLoopGroup eventLoopGroup;

    private final String host;
    private final int port;

    private final int maxRetries;

    public KairosdbClient(String host, int port, int maxConnections, int maxRetries) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.maxRetries = maxRetries;
        this.eventLoopGroup = new NioEventLoopGroup();

        Bootstrap bootstrap = createBootstrap();
        ChannelPoolHandler cph = new ChannelPoolHandler() {
            @Override
            public void channelReleased(Channel ch) throws Exception {
            }

            @Override
            public void channelAcquired(Channel ch) throws Exception {
            }

            @Override
            public void channelCreated(Channel ch) throws Exception {
                ch.pipeline().addLast("decoder", new StringDecoder())
                        .addLast("encoder", new StringEncoder());
            }
        };
        this.channelPool = new FixedChannelPool(bootstrap, cph, maxConnections);
    }

    public KairosdbClient(String host, int port) throws ExecutionException, InterruptedException {
        this(host, port, 8, 2);
    }

    private Bootstrap createBootstrap() {
        return new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(host, port)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) throws Exception {}
                })
                .validate();
    }


    public void put(Put put) {
        putAsync(put).awaitUninterruptibly();
    }

    public Promise<Void> putAsync(Put put) {
        String msg = put.cmdString();
        return maxRetries > 0
                ? retrySendAsync(msg)
                : sendAsync(msg);
    }

    public void put(List<Put> puts){
        putAsync(puts).awaitUninterruptibly();
    }

    public Promise<Void> putAsync(List<Put> puts){
        StringBuilder sb = new StringBuilder();
        puts.forEach(put -> put.writeTo(sb));
        String msg = sb.toString();
        return maxRetries > 0
                ? retrySendAsync(msg)
                : sendAsync(msg);
    }

    public void shutdown() throws InterruptedException {
        this.eventLoopGroup.shutdownGracefully();
        this.channelPool.close();
    }


    private Promise<Void> retrySendAsync(String msg){
        if (maxRetries > 0) {
            Promise<Void> promise = this.eventLoopGroup.next().newPromise();
            retrySendAsync(msg, promise, 0);
            return promise;
        } else {
            return sendAsync(msg);
        }
    }

    private void retrySendAsync(String msg, Promise<Void> promise, int retries){
        sendAsync(msg).addListener((FutureListener<Void>)f -> {
            if (f.isSuccess()){
                promise.setSuccess(f.get());
            } else {
                if (retries < maxRetries){
                    int nextRetry = retries + 1;
                    LOG.error("send msg error: " + msg + " , retry " + nextRetry + " times", f.cause());
                    this.eventLoopGroup.next().schedule(
                            () -> retrySendAsync(msg, promise, nextRetry),
                            retrySeconds(retries),
                            TimeUnit.SECONDS);
                } else {
                    promise.setFailure(f.cause());
                }
            }
        });
    }

    private static int retrySeconds(int retries){
        return (int) Math.pow(2, retries);
    }

    private Promise<Void> sendAsync(String msg) {
        Promise<Void> promise = this.eventLoopGroup.next().newPromise();
        Future<Channel> acquireFuture = this.channelPool.acquire();
        if(acquireFuture.isDone()){
            getChannelAndWrite(acquireFuture, msg, promise);
        }
        acquireFuture.addListener((FutureListener<Channel>)f -> getChannelAndWrite(f, msg, promise));

        return promise;
//                .addListener(f -> {
//                if (!f.isSuccess()){
//                    System.out.println(f.cause());
//                }
//        });
    }



    private void getChannelAndWrite(Future<Channel> acquireFuture, String msg, Promise<Void> sendPromise) {
        if (acquireFuture.isSuccess()) {
            Channel channel = getChannel(acquireFuture, sendPromise);
            if (channel != null) {
                writeAndFlush(channel, msg, sendPromise);
            }
        } else {
            sendPromise.setFailure(acquireFuture.cause());
        }
    }

    private Channel getChannel(Future<Channel> acquireFuture, Promise<Void> sendPromise){
        try {
            return acquireFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            sendPromise.setFailure(e);
        }
        return null;
    }

    private void writeAndFlush(Channel channel, String msg, Promise<Void> sendPromise) {
        ChannelFuture channelFuture = channel.writeAndFlush(msg);
        if (channelFuture.isDone()) {
            releaseChannelAndNotifyResult(channel, channelFuture, sendPromise);
        } else {
            channelFuture.addListener((FutureListener<Void>) future ->
                    releaseChannelAndNotifyResult(channel, future, sendPromise));
        }
    }

    private void releaseChannelAndNotifyResult(Channel channel, Future<Void> future, Promise<Void> sendPromise) {
        channelPool.release(channel).addListener(releaseFuture -> {
            notifyResult(future, sendPromise);
            logReleaseChannel(channel, releaseFuture);
        });
    }

    private void notifyResult(Future<Void> future, Promise<Void> sendPromise){
        if (future.isSuccess()) {
            sendPromise.setSuccess(future.getNow());
        } else {
            sendPromise.setFailure(future.cause());
        }
    }

    private void logReleaseChannel(Channel channel, Future<? super Void> releaseFuture) {
        if (!releaseFuture.isSuccess()){
            LOG.error("release channel error: " + channel, releaseFuture.cause());
        }
    }


    public static void main(String[] args) throws InterruptedException, ExecutionException {
//        KairosdbClient client = new KairosdbClient("10.103.17.90", 4242);
        KairosdbClient client = new KairosdbClient("localhost", 4242, 5, 3);

        Put put = Put.of("test", System.currentTimeMillis(), 1);
        Put put2 = Put.of("test", System.currentTimeMillis(), 1);
        client.put(put);

//
//        client.putAsync(put2).get();

        for (int i = 0; i < 100; i++) {
            client.put(Put.of("test", System.currentTimeMillis(), i));
            Thread.sleep(10000);
        }

        System.out.println("sleep ...");
        Thread.sleep(60000);
        System.out.println("close ...");
        client.shutdown();

    }
}
