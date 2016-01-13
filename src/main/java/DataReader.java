import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakRetryFailedException;

public class DataReader implements Runnable {

    private static int thrId = 0;

    private Record record;
    private int threadId;

    private boolean finished = false;

    private IRiakClient client;
    private String bucket;

    public static void resetIdIterator() {
        thrId = 0;
    }

    public DataReader(Record record, IRiakClient client, String bucket) {
        this.record = record;
        this.client = client;
        this.bucket = bucket;
        this.threadId = thrId++;
    }

    public void run() {
        try {
            Statistics statistics = StatisticsManager.getInstance().getStatistics();
            statistics.startMeasure();
            System.out.println(readFromDB(client, bucket, record.timestamp));
            statistics.stopMeasure();
        } catch (RiakRetryFailedException e) {
            e.printStackTrace();
        }finally {
            this.client.shutdown();
        }
    }

    public static String readFromDB(IRiakClient myHttpClient,String bucket, String key) throws RiakRetryFailedException {
        return myHttpClient.fetchBucket(bucket).execute().fetch(key).execute().getValueAsString();
    }

}