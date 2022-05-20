package com.learning.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;


public class MyRunnable implements Runnable      //(содержащее метод run())
{

    private Iterator<String> myIterator;
    // private  Channel channel;

    public MyRunnable() throws IOException, TimeoutException {
    }

    public void run()        //Этот метод будет выполняться в побочном потоке
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = null;

        try {
            conn = factory.newConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        Channel channel = null;
        try {
            channel = conn.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        GetResponse raw_url = null; // запрос из которого необходимо вытащить только его наполнение , то есть локальную ссылку
        while (true) {
            try {
                raw_url = (channel.basicGet(SimpleParser.PRODUCER_QUEUE, false)); //basicGet :имя очереди на rabbit'e, autoAck подтверждение доставки сообщение или нет
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (raw_url.getMessageCount()>=0) {
                String localurl = new String(raw_url.getBody(), StandardCharsets.UTF_8);
                try {
                    SimpleParser.ParsingNews(localurl);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}