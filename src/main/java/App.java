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

public class App
{
    private static final int recordsSize = 10;

    static String host1 = "10.156.207.9";
    static String host2 = "10.156.207.150";
    static String port = "8098";
    static int maxConnections = 64;
    static String bucket = "test1";
    static LinkedList<Record> recordList;
    static LinkedList<Record> recordList2;
    static HTTPClusterConfig myHttpClusterConfig;


    public static void main(String[] args) throws RiakException, InterruptedException {
        createRecordList();
        createConfig();
//        IRiakClient myHttpClient = RiakFactory.newClient(myHttpClusterConfig);
//        System.out.println(readFromDB(myHttpClient, "big"));
//        myHttpClient.shutdown();

        writeTestCase(8, false);
        readTestCase(8);

    }

    public static void createConfig() throws RiakException {
        myHttpClusterConfig = new HTTPClusterConfig(maxConnections);
        HTTPClientConfig myHttpClientConfig3 = HTTPClientConfig.defaults();
        myHttpClusterConfig.addHosts(myHttpClientConfig3, host1, host2);
    }

    public static void writeTestCase(int threadsNb, boolean deleteAtEnd) throws RiakException, InterruptedException {

        addData(threadsNb, "WO" + threadsNb + "Thread-empty.txt", "", recordList);
        DataWriter.resetIdIterator();
        addData(threadsNb, "WO" + threadsNb + "Thread-full.txt", "Second", recordList2);
        DataWriter.resetIdIterator();

        if (deleteAtEnd) {
            deleteAll(RiakFactory.newClient(myHttpClusterConfig));
        }
    }

    public static void readTestCase(int threadsNb) throws RiakException, InterruptedException {

        readData(threadsNb, "RO" + threadsNb + "Linear.txt", "Linear", recordList);
        DataWriter.resetIdIterator();
        readDataRandom(threadsNb, "RO" + threadsNb + "Random.txt", "Random", recordList);
        DataWriter.resetIdIterator();
    }

    public static void readData(int threadsNb, String filename, String postfix, LinkedList<Record> recordList) throws RiakException, InterruptedException {
        System.out.println("Read " + threadsNb + " " + postfix);
        StatisticsManager.getInstance().reset();
        DataReader.resetIdIterator();

        ExecutorService exec = Executors.newFixedThreadPool(threadsNb);
        for(Record data: recordList) {
            exec.submit(new DataReader(data,RiakFactory.newClient(myHttpClusterConfig),bucket));
        }
        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.MINUTES);

        StatisticsManager sm = StatisticsManager.getInstance();
        sm.sumUp();
        sm.writeToFile(filename);
    }

    public static void readDataRandom(int threadsNb, String filename, String postfix, LinkedList<Record> recordList) throws RiakException, InterruptedException {
        System.out.println("Read " + threadsNb + " " + postfix);
        StatisticsManager.getInstance().reset();
        DataReader.resetIdIterator();
        LinkedList<Record> copy = new LinkedList(recordList);
        ExecutorService exec = Executors.newFixedThreadPool(threadsNb);
        while(!copy.isEmpty()){
            int index = ThreadLocalRandom.current().nextInt(0, copy.size());
            Record data = copy.get(index);
            copy.remove(index);
            exec.submit(new DataReader(data, RiakFactory.newClient(myHttpClusterConfig), bucket));
        }
        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.MINUTES);

        StatisticsManager sm = StatisticsManager.getInstance();
        sm.sumUp();
        sm.writeToFile(filename);
    }

    public static void addData(int threadsNb, String filename, String postfix, LinkedList<Record> recordList) throws RiakException, InterruptedException {
        System.out.println("Write " + threadsNb + " " + postfix);
        StatisticsManager.getInstance().reset();
        DataWriter.resetIdIterator();

        ExecutorService exec = Executors.newFixedThreadPool(threadsNb); // 4 threads
        for(Record data: recordList) {
            exec.submit(new DataWriter(data,RiakFactory.newClient(myHttpClusterConfig),bucket));
        }

        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.MINUTES);

        StatisticsManager sm = StatisticsManager.getInstance();
        sm.sumUp();
        sm.writeToFile(filename);
    }

    public static void deleteAll(IRiakClient myHttpClient) throws RiakException {
        deleteRecordList(myHttpClient,recordList);
        deleteRecordList(myHttpClient,recordList2);
    }

    public static void deleteRecordList(IRiakClient myHttpClient,LinkedList<Record> recordList) throws RiakException {
        for(Record r : recordList){
            deleteFromDB(myHttpClient,r.timestamp);
        }
    }

    public static void deleteFromDB(IRiakClient myHttpClient, String key) throws RiakException {
        myHttpClient.fetchBucket(bucket).execute().delete(key).execute();
    }


    public static String readFromDB(IRiakClient myHttpClient, String key) throws RiakRetryFailedException {
        return myHttpClient.fetchBucket(bucket).execute().fetch(key).execute().getValueAsString();
    }



    public static void createRecordList(){
        recordList = new LinkedList<>();
        for(int i =0; i<recordsSize; i++){
            recordList.add(new Record());
        }
        recordList2 = new LinkedList<>();
        for(int i =0; i<recordsSize; i++){
            recordList2.add(new Record());
        }
    }



}