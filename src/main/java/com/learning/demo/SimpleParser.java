package com.learning.demo;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SimpleParser {

    final static String URL = "https://trashbox.ru";
    static Boolean Flag_Down = false;
    static MyRunnable mThing;

    public static void main(String[] args) throws IOException, InterruptedException {
//        List<DescriptionNews> descriptionNews = new ArrayList<>();
        List<String> firstList = new CopyOnWriteArrayList<>(); // лист с ссылками, которые считал главный поток

        Document doc = null;
        try (CloseableHttpClient client = HttpClients.createDefault(); // создание закрытого клеинта, который будет кидать запросы
             CloseableHttpResponse response = client.execute(new HttpGet(URL))) /* отправка запроса от клиента по заданной ссылке */ {
            if (response.getStatusLine().getStatusCode() != 200) {
                Flag_Down = true;
            }

            if (Flag_Down == false) {
                HttpEntity entity = response.getEntity(); // заворачивание ответа от сервера в сущность
                doc = Jsoup.parse(entity.getContent(), "UTF-8", URL);
            }
        }


        Elements ListNews = doc.getElementsByAttributeValue("class", "a_topic_cover");

        for (int i = 0; i < ListNews.size(); i++) {
            firstList.add(URL + ListNews.get(i).attr("href")); // заполнение листа главным потоком
        }
        // ----------------------------------------------------------------------------------------------------------

        mThing = new MyRunnable(firstList);
        Thread myThread1 = new Thread(mThing);
        Thread myThread2 = new Thread(mThing);
        Thread myThread3 = new Thread(mThing);

        myThread1.start();
        myThread2.start();
        myThread3.start();

    }

    public static void ParsingNews(String localUrl) throws IOException {
        Document doc = null;
        int code = 0;
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(new HttpGet(localUrl))) {
            code = response.getStatusLine().getStatusCode();
            if (code == 403) {
//                System.out.println("ERROR:" + code + " from:" + localUrl);
//                response.close();
//                response = client.execute(new HttpGet(localUrl));
            } else if (code != 200) {
                Flag_Down = true;
            }

            if (Flag_Down == false) {
                HttpEntity entity = response.getEntity(); // заворачивание ответа от сервера в сущность
                doc = Jsoup.parse(entity.getContent(), "UTF-8", localUrl);
            } else {
                System.out.println("ERROR:" + code + " from:" + localUrl);
                return;
            }

            code = response.getStatusLine().getStatusCode();
        }
        Elements CurNews = doc.getElementsByAttributeValue("itemprop", "mainEntity");
        Elements TextNews = doc.getElementsByClass("div_text_content nopadding");//!заготовка под парсинг текста!
        DescriptionNews NowNews = new DescriptionNews();

        NowNews.author = CurNews.attr("data-author-login");
        NowNews.author_link = URL + CurNews.select("a[href]").attr("href");
        NowNews.date_first = CurNews.select("time").first().text();
        NowNews.date_change = CurNews.select("time").last().text();
        NowNews.title = CurNews.select("h1").first().text();
        NowNews.link = localUrl;
        NowNews.main_text = CurNews.select("p").text();


        System.out.println("Title:" + NowNews.title + " | " + NowNews.link
                + "\nAuthor:" + NowNews.author + "\tLink on Author:" + NowNews.author_link
                + "\nDate Publish:" + NowNews.date_first
                + "\nDate Change:" + NowNews.date_change
                + "\n" + NowNews.main_text
                + "\nCode:" + code
                + "\nPars by " + Thread.currentThread().getName()
                + "\n");
    }

}






