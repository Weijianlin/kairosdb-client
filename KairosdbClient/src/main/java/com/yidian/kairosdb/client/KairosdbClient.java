package com.yidian.kairosdb.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.internal.SystemPropertyUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author weijian
 * Date : 2015-12-14 20:53
 */

public class KairosdbClient {

    private final Bootstrap bootstrap;
    private final Channel channel;
    private final NioEventLoopGroup group;

    public KairosdbClient(String host, int port) throws InterruptedException {

        group = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();
        this.bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast("decoder", new StringDecoder());
                        ch.pipeline().addLast("encoder", new StringEncoder());
//                        ch.pipeline().addLast("handler", new KairosdbCmdHandler());
                    }
                });

        // Start the connection attempt.
        this.channel = this.bootstrap.connect(host, port).sync().channel();

    }

    public void put(Put put) {
        this.channel.writeAndFlush(put.cmdString()).syncUninterruptibly();
    }

    public void put(List<Put> puts) {
        StringBuilder sb = new StringBuilder();
        puts.forEach(put -> put.writeTo(sb));
        this.channel.writeAndFlush(sb.toString()).syncUninterruptibly();
    }

    public ChannelFuture putAsync(List<Put> puts){
        StringBuilder sb = new StringBuilder();
        puts.forEach(put -> put.writeTo(sb));
        return this.channel.writeAndFlush(sb.toString());
    }


    public void shutdown() throws InterruptedException {
        this.channel.close().sync();
        this.group.shutdownGracefully();
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        KairosdbClient client = new KairosdbClient("10.103.17.96", 4242);
//        KairosdbClient client = new KairosdbClient("localhost", 4242);

        Map<String, String> map = new HashMap<>();
        map.put("tag", "a");
        map.put("tag", "b");

        long ts = 1454658805000L;
        Put put = Put.of("test", ts, 9000, map);
        Put put2 = Put.of("test", ts - 10000, 6000);

        System.out.println(put.cmdString());
        System.out.println(put2.cmdString());
        client.put(put);
        client.put(put2);
////
        long ts1 = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            if (i %100 == 0) System.out.println(i);
            client.put(new ArrayList<Put>(){{add(put); add(put2);add(put); add(put2);}});
        }
        long ts2 = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            client.put(new ArrayList<Put>() {{
                add(put);
                add(put2);
                add(put);
                add(put2);
            }});
        }
        long ts3 = System.currentTimeMillis();

        System.out.println(ts2 - ts1);
        System.out.println(ts3 - ts2);

        Thread.sleep(5000);
        client.shutdown();
    }
}
