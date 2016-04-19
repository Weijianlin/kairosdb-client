package com.yidian.kairosdb.client;


import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class KairosdbCmdHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.printf("......");
        System.out.println(msg);
        if (msg instanceof String)
        {
            System.out.println(msg);
        }
    }
}