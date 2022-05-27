package com.learning.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.util.CoreMap;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.BasicConfigurator;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.client.transport.TransportClient;
import edu.stanford.nlp.pipeline.*;

import java.util.*;

import static java.awt.SystemColor.text;


public class SimpleParser {

    final static String PRODUCER_QUEUE = "Producer_Queue";
    final static String URL = "https://trashbox.ru";
    static Boolean Flag_Down = false;
    static MyRunnable mThing;
    private static CloseableHttpClient client;

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException, ExecutionException {
//        List<DescriptionNews> descriptionNews = new ArrayList<>();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare(PRODUCER_QUEUE, false, false, false, null);//название очереди,долговечность, уникальность, Автоудаление, какой-то объект

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
            channel.basicPublish("", PRODUCER_QUEUE, null, firstList.get(i).getBytes(StandardCharsets.UTF_8));
        }
        channel.close();
        conn.close();
        // ----------------------------------------------------------------------------------------------------------

        mThing = new MyRunnable();
        Thread consumer1 = new Thread(mThing);
        Thread consumer2 = new Thread(mThing);
        Thread consumer3 = new Thread(mThing);

        consumer1.start();
        consumer2.start();
        consumer3.start();

        TimeUnit.SECONDS.sleep(20); // ожидание заполнения бд

        cluster_and_aggreg();
        definition_speak();
    }

    public static void definition_speak() throws UnknownHostException, ExecutionException, InterruptedException {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        // https://github.com/MANASLU8/CoreNLP
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        props.setProperty("tokenize.language", "en");
        props.setProperty("pos.model", "C:\\Users\\mbond\\IdeaProjects\\HelloWorld\\RusStanNLP\\edu\\stanford\\nlp\\models\\pos-tagger\\russian-ud-pos.tagger");
        props.setProperty("customAnnotatorClass.custom.lemma", "C:\\Users\\mbond\\IdeaProjects\\HelloWorld\\RusStanNLP\\edu\\stanford\\nlp\\international\\russian\\process\\RussianLemmatizationAnnotator.class");
        props.setProperty("custom.lemma.dictionaryPath", "C:\\Users\\mbond\\IdeaProjects\\HelloWorld\\RusStanNLP\\edu\\stanford\\nlp\\international\\russian\\process\\dict.tsv");
        props.setProperty("customAnnotatorClass.custom.morpho", "C:\\Users\\mbond\\IdeaProjects\\HelloWorld\\RusStanNLP\\edu\\stanford\\nlp\\international\\russian\\process\\RussianMorphoAnnotator.class");
        props.setProperty("custom.morpho.model", "C:\\Users\\mbond\\IdeaProjects\\HelloWorld\\RusStanNLP\\edu\\stanford\\nlp\\models\\pos-tagger\\russian-ud-mf.tagger");
        props.setProperty("depparse.model", "C:\\Users\\mbond\\IdeaProjects\\HelloWorld\\RusStanNLP\\edu\\stanford\\nlp\\models\\parser\\nndep\\nndep.rus.model.wiki.txt.gz");
        props.setProperty("depparse.language", "russian");

        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        TermsAggregationBuilder aggregation_builder = AggregationBuilders.terms("title_count").field("title.keyword");

        TransportClient client_get = new PreBuiltTransportClient(
                Settings.builder().put("cluster.name", "docker-cluster").build()).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        SearchSourceBuilder aggregation_search_src = new SearchSourceBuilder().aggregation(aggregation_builder);
        SearchRequest aggregation_search_request = new SearchRequest().indices("news").source(aggregation_search_src);
        SearchResponse aggregation_search_response = client_get.search(aggregation_search_request).get();

        Terms terms = aggregation_search_response.getAggregations().get("title_count");

        String text;
        // create an empty Annotation just with the given text

        for (int count = 0; count < terms.getBuckets().size(); count++) {

            text = String.valueOf(terms.getBuckets().get(count).getKey());

            Annotation document = new Annotation(text);

            // run all Annotators on this text
            pipeline.annotate(document);

            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
            System.out.println(String.format("Анализ заголовка новости № %d",count+1));
            for (CoreMap sentence : sentences) {
                // traversing the words in the current sentence
                // a CoreLabel is a CoreMap with additional token-specific methods
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    // this is the text of the token
                    String word = token.get(CoreAnnotations.TextAnnotation.class);
                    // this is the POS tag of the token
                    String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                    System.out.println(String.format("Слово:%20s Часть речи: %s", word, pos));
                }
            }
            System.out.println("");
        }
    }

    public static void cluster_and_aggreg() throws ExecutionException, InterruptedException, UnknownHostException {

        TransportClient client_get = new PreBuiltTransportClient(
                Settings.builder().put("cluster.name", "docker-cluster").build()).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));


        BoolQueryBuilder bool_quare = QueryBuilders.boolQuery();
        MatchQueryBuilder match_quare = QueryBuilders.matchQuery("title", "Обзор");

        bool_quare.must(match_quare);
        SearchSourceBuilder search_src = new SearchSourceBuilder().query(match_quare).size(1000);
        SearchRequest search_request = new SearchRequest().indices("news").source(search_src);

        SearchHit[] search_hits = client_get.search(search_request).get().getHits().getHits();
        List list_hits = Arrays.stream(search_hits).toList();
        System.out.println("id новостей, в которых в названии упоминается 'Обзор':");

        for (int count = 0; count < search_hits.length; count++) {
            String test = list_hits.get(count).toString();

            int start = 45;
            int end = 87;
            char[] dst = new char[end - start];
            test.getChars(start, end, dst, 0);
            System.out.println(dst);
        }

        System.out.println("");
        TermsAggregationBuilder aggregation_builder = AggregationBuilders.terms("author_count").field("author.keyword");

        SearchSourceBuilder aggregation_search_src = new SearchSourceBuilder().aggregation(aggregation_builder);
        SearchRequest aggregation_search_request = new SearchRequest().indices("news").source(aggregation_search_src);
        SearchResponse aggregation_search_response = client_get.search(aggregation_search_request).get();

        Terms terms = aggregation_search_response.getAggregations().get("author_count");
        for (int count = 0; count < terms.getBuckets().size(); count++) {

            System.out.printf("Частота появляения автора: [%s] in ES = %s\n", terms.getBuckets().get(count).getKey()
                    , terms.getBuckets().get(count).getDocCount());
        }
        client_get.close();
    }

    public static DescriptionNews ParsingNews(String localUrl) throws IOException {
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
                return null;
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

//        System.out.println("Title:" + NowNews.title + " | " + NowNews.link
//                + "\nAuthor:" + NowNews.author + "\tLink on Author:" + NowNews.author_link
//                + "\nDate Publish:" + NowNews.date_first
//                + "\nDate Change:" + NowNews.date_change
//                + "\n" + NowNews.main_text
//                + "\nCode:" + code
//                + "\nPars by " + Thread.currentThread().getName()
//                + "\n");

        return NowNews;
    }
}