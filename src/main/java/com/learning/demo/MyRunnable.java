package com.learning.demo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.learning.demo.SimpleParser.ParsingNews;

class MyRunnable            //Нечто, реализующее интерфейс Runnable
        implements Runnable        //(содержащее метод run())
{
    private List<String> localList = new CopyOnWriteArrayList<>();
    private Iterator<String> myIterator;

    public MyRunnable(List<String> _localList) throws IOException {
        this.localList = _localList;
    }

    public void run()        //Этот метод будет выполняться в побочном потоке
    {
        myIterator = localList.iterator();

        while (myIterator.hasNext()) {
            String localUrl = myIterator.next();
            localList.remove(0);
            try {
                ParsingNews(localUrl);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}