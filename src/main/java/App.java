import com.basho.riak.client.*;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;

public class App
{
    static String host1 = "10.156.207.9";
    static String host2 = "10.156.207.150";
    static String port = "8098";
    static int maxConnections = 64;
    static String bucket = "test";
    static LinkedList<Record> recordList;

    public static void main(String[] args) throws RiakException
    {
        createRecordList();

        HTTPClusterConfig myHttpClusterConfig = new HTTPClusterConfig(maxConnections);
        HTTPClientConfig myHttpClientConfig3 = HTTPClientConfig.defaults();
        myHttpClusterConfig.addHosts(myHttpClientConfig3, host1, host2);
        IRiakClient myHttpClient = RiakFactory.newClient(myHttpClusterConfig);

        System.out.println(readFromDB(myHttpClient, "big"));

        myHttpClient.shutdown();

    }

    public static String readFromDB(IRiakClient myHttpClient, String key) throws RiakRetryFailedException {
        return myHttpClient.fetchBucket(bucket).execute().fetch(key).execute().getValueAsString();
    }

    public static void writeToDB(IRiakClient myHttpClient, String key, String value) throws RiakRetryFailedException {
        myHttpClient.fetchBucket(bucket).execute().store(key, value).execute();
    }

    public static void createRecordList(){
        recordList = new LinkedList<>();
        for(int i =0; i<40; i++){
            recordList.add(new Record());
        }
    }



}