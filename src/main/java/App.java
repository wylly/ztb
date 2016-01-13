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

public class App
{
    static String host1 = "10.156.207.9";
    static String host2 = "10.156.207.150";
    static String port = "8098";
    static int maxConnections = 64;
    static String bucket = "test";
    public static void main(String[] args) throws RiakException
    {

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

    public static String createRecord(){
        java.util.Date date= new java.util.Date();
        String currTimeStamp = (new Timestamp(date.getTime())).toString();
        String record = "{" +
                "\"Timestamp\":"+"\""+currTimeStamp+"\",\n"+
                "\"URI\":"+"\""+longURL+"\""+",\n"+
                "\"BigInt1\":"+"\""+bigInt+"\""+",\n"+
                "\"BigInt2\":"+"\""+bigInt+"\""+",\n"+
                "\"BigInt3\":"+"\""+bigInt+"\""+",\n"+
                "\"BigInt4\":"+"\""+bigInt+"\""+",\n"+
                "\"BigDouble1\":"+"\""+bigDouble+"\""+",\n"+
                "\"BigDouble2\":"+"\""+bigDouble+"\""+",\n"+
                "\"BigDouble4\":"+"\""+bigDouble+"\""+",\n"+
                "\"BigDouble3\":"+"\""+bigDouble+"\""+",\n"+
                "\"longText\":"+"\""+longText+"\""+",\n"+
                "}";
        return record;
    }

    public static String readFromFile(){
        String CurrentLine="";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("C:\\Users\\Bartek\\IdeaProjects\\ztb\\src\\main\\resources\\longText.txt"));
            CurrentLine = br.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return CurrentLine;
    }

    static String longURL = "http://developers.jollypad.com/fb/index.php?dmmy=1&fb_sig_in_iframe=1&fb_sig_iframe_key=8e296a067a37563370ded05f5a3bf3ec&fb_sig_locale=bg_BG&fb_sig_in_new_facebook=1&fb_sig_time=1282749119.128&fb_sig_added=1&fb_sig_profile_update_time=1229862039&fb_sig_expires=1282755600&fb_sig_user=761405628&fb_sig_session_key=2.IuyNqrcLQaqPhjzhFiCARg__.3600.1282755600-761405628&fb_sig_ss=igFqJKrhJZWGSRO__Vpx4A__&fb_sig_cookie_sig=a9f110b4fc6a99db01d7d1eb9961fca6&fb_sig_ext_perms=user_birthday,user_religion_politics,user_relationships,user_relationship_details,user_hometown,user_location,user_likes,user_activities,user_interests,user_education_history,user_work_history,user_online_presence,user_website,user_groups,user_events,user_photos,user_videos,user_photo_video_tags,user_notes,user_about_me,user_status,friends_birthday,friends_religion_politics,friends_relationships,friends_relationship_details,friends_hometown,friends_location,friends_likes,friends_activities,friends_interests,friends_education_history,friends_work_history,friends_online_presence,friends_website,friends_groups,friends_events,friends_photos,friends_videos,friends_photo_video_tags,friends_notes,friends_about_me,friends_status&fb_sig_country=bg&fb_sig_api_key=9f7ea9498aabcd12728f8e13369a0528&fb_sig_app_id=177509235268&fb_sig=1a5c6100fa19c1c9b983e2d6ccfc05ef";
    static String longText = readFromFile();
    static int bigInt = Integer.MAX_VALUE;
    static double bigDouble = Double.MAX_VALUE;
}