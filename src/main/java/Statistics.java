import java.util.LinkedList;
import java.util.List;

/**
 * Created by Mikolaj on 2016-01-11.
 */
public class Statistics {
    private long firstStartTime = -1;
    private long lastStartTime;
    private long lastEndTime;
    List<Long> measures = new LinkedList<Long>();


    public Statistics(){
    }

    public void startMeasure(){
        lastStartTime = System.currentTimeMillis();
        if (firstStartTime == -1){
            firstStartTime = lastStartTime;
        }
    }

    public void stopMeasure(){
        lastEndTime = System.currentTimeMillis();
        measures.add(lastEndTime - lastStartTime);
    }

    public long getFirstStartTime() {
        return firstStartTime;
    }

    public long getLastEndTime() {
        return lastEndTime;
    }
}
