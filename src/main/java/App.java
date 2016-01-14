import com.basho.riak.client.*;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.sun.java.accessibility.util.EventID;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class App {
    private static final int recordsSize = 1000;

//    static String host1 = "10.156.207.9";
//    static String host2 = "10.156.207.150";
static String host1 = "10.156.207.9";
    static String host2 = "10.156.207.175";
    static String host3 = "10.156.207.172";
    static String port = "8098";
    static int maxConnections = 64;
    static String bucket = "test3";
    static LinkedList<Record> recordList;
    static LinkedList<Record> recordList2;
    static HTTPClusterConfig myHttpClusterConfig;


    public static void main(String[] args) throws RiakException, InterruptedException {
        createRecordList();
        createConfig();
//        IRiakClient myHttpClient = RiakFactory.newClient(myHttpClusterConfig);
//        System.out.println(readFromDB(myHttpClient, "big"));
//        myHttpClient.shutdown();
        writeTestCase(4, false);
        readTestCase(4);
        int[] rThreads = {1,2};
        int[] wThreads = {1,2};
//        readWriteTestCase(rThreads,wThreads,true);
    }

    public static void createConfig() throws RiakException {
        myHttpClusterConfig = new HTTPClusterConfig(maxConnections);
        HTTPClientConfig myHttpClientConfig3 = HTTPClientConfig.defaults();
        myHttpClusterConfig.addHosts(myHttpClientConfig3, host1, host2, host3);
    }

    public static void readWriteTestCase(int[] rThreadsNb,int[] wThreadNb, boolean deleteAtEnd) throws RiakException, InterruptedException {

        for(int rThreads: rThreadsNb){
            for(int wThreads: wThreadNb){
                rwData(rThreads, wThreads, "R" + rThreads +"W"+ wThreads + "Threads.txt", "", recordList,recordList2);
                DataWriter.resetIdIterator();
            }
        }
        if (deleteAtEnd) {
            deleteAll(RiakFactory.newClient(myHttpClusterConfig));
        }
    }

    public static void writeTestCase(int threadsNb, boolean deleteAtEnd) throws RiakException, InterruptedException {

        addData(threadsNb, "WO" + threadsNb + "Thread-empty.txt", "", recordList);
        DataWriter.resetIdIterator();
//        addData(threadsNb, "WO" + threadsNb + "Thread-full.txt", "Second", recordList2);
//        DataWriter.resetIdIterator();
//
//        if (deleteAtEnd) {
//            deleteRecordList(RiakFactory.newClient(myHttpClusterConfig),recordList2);
//        }
    }

    public static void rwData(int rThreads, int wThreads, String filename, String postfix, LinkedList<Record> rRecordList,LinkedList<Record> wRecordList) throws RiakException, InterruptedException {
        System.out.println("Read " + rThreads + " Write "+wThreads + postfix);
        StatisticsManager.getInstance().reset();
        DataReader.resetIdIterator();
        LinkedList[] rdata = splitData(rRecordList, rThreads);
        List<DataReader> readers = new ArrayList<>();
        StatisticsManager.start();
        DataWriter.resetIdIterator();
        List<DataWriter> dataWriters = new ArrayList<>();
        LinkedList[] wdata = splitData(wRecordList, wThreads);
        StatisticsManager.start();
        for (int i =0; i < wThreads; i++) {
            DataWriter dataWriter = new DataWriter(wdata[i], RiakFactory.newClient(myHttpClusterConfig), bucket);
            new Thread(dataWriter).start();
            dataWriters.add(dataWriter);
        }
        for (int i = 0; i < rThreads; i++) {
            DataReader reader = new DataReader(rdata[i], RiakFactory.newClient(myHttpClusterConfig), bucket, false);
            new Thread(reader).start();
            readers.add(reader);
        }
        for (DataWriter dataWriter: dataWriters) {
            dataWriter.waitUntilEnd();
        }
        for (DataReader reader: readers) {
            reader.waitUntilEnd();
        }
        StatisticsManager.stop();
        StatisticsManager sm = StatisticsManager.getInstance();
        sm.sumUp();
        sm.writeToFile(filename);
    }

    public static void readTestCase(int threadsNb) throws RiakException, InterruptedException {

        readData(threadsNb, "RO" + threadsNb + "Linear.txt", "Linear", recordList, false);
        DataWriter.resetIdIterator();
//        readData(threadsNb, "RO" + threadsNb + "Random.txt", "Random", recordList, true);
//        DataWriter.resetIdIterator();
    }


    public static void readData(int threadsNb, String filename, String postfix, LinkedList<Record> recordList, boolean isRandom) throws RiakException, InterruptedException {
        System.out.println("Read " + threadsNb + " " + postfix);
        StatisticsManager.getInstance().reset();
        DataReader.resetIdIterator();
        LinkedList[] data = splitData(recordList, threadsNb);
        List<DataReader> readers = new ArrayList<>();
        StatisticsManager.start();
        for (int i = 0; i < threadsNb; i++) {
            DataReader reader = new DataReader(data[i], RiakFactory.newClient(myHttpClusterConfig), bucket, isRandom);
            new Thread(reader).start();
            readers.add(reader);
        }
        for (DataReader reader: readers) {
            reader.waitUntilEnd();
        }
        StatisticsManager.stop();

        StatisticsManager sm = StatisticsManager.getInstance();
        sm.sumUp();
        sm.writeToFile(filename);
    }

    public static void addData(int threadsNb, String filename, String postfix, LinkedList<Record> recordList) throws RiakException, InterruptedException {
        System.out.println("Write " + threadsNb + " " + postfix);
        StatisticsManager.getInstance().reset();
        DataWriter.resetIdIterator();
        List<DataWriter> dataWriters = new ArrayList<>();
        LinkedList[] data = splitData(recordList, threadsNb);
        StatisticsManager.start();
        for (int i =0; i < threadsNb; i++) {
            DataWriter dataWriter = new DataWriter(data[i], RiakFactory.newClient(myHttpClusterConfig), bucket);
            new Thread(dataWriter).start();
            dataWriters.add(dataWriter);
        }
        for (DataWriter dataWriter: dataWriters) {
            dataWriter.waitUntilEnd();
        }
        StatisticsManager.stop();
        StatisticsManager sm = StatisticsManager.getInstance();
        sm.sumUp();
        sm.writeToFile(filename);
    }

    public static void deleteAll(IRiakClient myHttpClient) throws RiakException {
        deleteRecordList(myHttpClient, recordList);
        deleteRecordList(myHttpClient, recordList2);
    }

    public static void deleteRecordList(IRiakClient myHttpClient, LinkedList<Record> recordList) throws RiakException {
        for (Record r : recordList) {
            deleteFromDB(myHttpClient, r.timestamp);
        }
    }

    public static void deleteFromDB(IRiakClient myHttpClient, String key) throws RiakException {
        myHttpClient.fetchBucket(bucket).execute().delete(key).execute();
    }


    public static String readFromDB(IRiakClient myHttpClient, String key) throws RiakRetryFailedException {
        return myHttpClient.fetchBucket(bucket).execute().fetch(key).execute().getValueAsString();
    }

    public static void createRecordList() throws InterruptedException {
        recordList = new LinkedList<>();
        for (int i = 0; i < recordsSize; i++) {
            recordList.add(new Record());
            Thread.sleep(1);
        }
        recordList2 = new LinkedList<>();
        for (int i = 0; i < recordsSize; i++) {
            recordList2.add(new Record());
            Thread.sleep(1);
        }
    }

    public static LinkedList<Record>[] splitData(LinkedList<Record> data, int threads) {
        LinkedList<Record>[] result = new LinkedList[threads];
        int partitionSize = (int) Math.floor(((double) data.size()) / threads);
        for (int i = 0; i < threads; i++) {
            int from = i*partitionSize;
            int to = from+partitionSize;
            if(to>data.size()-1) to = data.size();
            if(i==threads-1) to = data.size();
            result[i] = new LinkedList<>((data.subList(from,to)));
        }
        return result;
    }

}