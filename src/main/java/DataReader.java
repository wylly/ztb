import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class DataReader implements Runnable {

    private static int thrId = 0;

    private LinkedList<Record> record;
    private int threadId;
    private boolean isRandom = false;
    private boolean finished = false;

    private IRiakClient client;
    private String bucket;

    public static void resetIdIterator() {
        thrId = 0;
    }

    public DataReader(LinkedList<Record> record, IRiakClient client, String bucket, boolean isRandom) {
        this.record = record;
        this.client = client;
        this.bucket = bucket;
        this.threadId = thrId++;
        this.isRandom = isRandom;
    }

    public void run() {
        if(isRandom){
            try {
                LinkedList<Record> copy = new LinkedList(record);
                StatisticsManager.start();
                Statistics statistics = StatisticsManager.getInstance().getStatistics();
                while (!copy.isEmpty()) {
                    int index = ThreadLocalRandom.current().nextInt(0, copy.size());
                    Record data = copy.get(index);
                    statistics.startMeasure();
                    System.out.println(readFromDB(client, bucket, data.timestamp));
                    statistics.stopMeasure();
                    copy.remove(index);
                }
            } catch (RiakRetryFailedException e) {
                e.printStackTrace();
            }finally {
                this.client.shutdown();
            }
        }else{
            try {
                Statistics statistics = StatisticsManager.getInstance().getStatistics();
                for(Record r : record){
                    statistics.startMeasure();
                    System.out.println(readFromDB(client, bucket, r.timestamp));
                    statistics.stopMeasure();
                }
            } catch (RiakRetryFailedException e) {
                e.printStackTrace();
            }finally {
                this.client.shutdown();
            }
        }
        releaseWaiting();
    }

    public static String readFromDB(IRiakClient myHttpClient,String bucket, String key) throws RiakRetryFailedException {
        return myHttpClient.fetchBucket(bucket).execute().fetch(key).execute().getValueAsString();
    }
    synchronized private void releaseWaiting(){
        finished = true;
        notifyAll();
    }

    synchronized public void waitUntilEnd(){
        while (!finished){
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}