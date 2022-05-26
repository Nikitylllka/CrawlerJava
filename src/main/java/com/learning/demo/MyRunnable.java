package com.learning.demo;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xcontent.XContentType;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;


public class MyRunnable implements Runnable      //(содержащее метод run())
{

    private Iterator<String> myIterator;
    private Client client;
    private TransportClient client_es;

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
        DescriptionNews StructReport = new DescriptionNews();

        try {
            client_es = new PreBuiltTransportClient(
                    Settings.builder().put("cluster.name", "docker-cluster").build()).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

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

            if (raw_url.getMessageCount() >= 0) {
                String localurl = new String(raw_url.getBody(), StandardCharsets.UTF_8);
                try {
                    StructReport = SimpleParser.ParsingNews(localurl);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            String json = new Gson().toJson(StructReport);
            MessageDigest md = null;
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            md.update(json.getBytes());
            byte byteData[] = md.digest();
            StringBuffer hexString = new StringBuffer();
            for (byte aByteData : byteData) {
                String hex = Integer.toHexString(0xff & aByteData);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            String id = String.valueOf(hexString);

            client_es.prepareIndex("news", "_doc", id).setSource(json, XContentType.JSON).execute().actionGet();
        }
    }
}
