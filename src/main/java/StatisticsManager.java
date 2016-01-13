import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Mikolaj on 2016-01-11.
 */
public class StatisticsManager {

    private static StatisticsManager statisticsManager = new StatisticsManager();

    private List<Statistics> createdStatistics;

    private long minMeasure;
    private long maxMeasure;
    private long medianMeasure;
    private long measureSum;
    private double meanMeasure;
    private double recordsPerSecond = 0;

    private StatisticsManager(){
        createdStatistics = new LinkedList<Statistics>();
    }

    public static StatisticsManager getInstance(){
        return statisticsManager;
    }

    public Statistics getStatistics(){
        Statistics statistics = new Statistics();
        createdStatistics.add(statistics);
        return statistics;
    }

    public void sumUp(){
        LinkedList<Long> measures = new LinkedList<Long>();
        long earliestStart = Long.MAX_VALUE;
        long latestEnd = -1;
        for (Statistics statistics: createdStatistics){
            if (statistics.getFirstStartTime() < earliestStart){
                earliestStart = statistics.getFirstStartTime();
            }
            if (statistics.getLastEndTime() > latestEnd) {
                latestEnd = statistics.getLastEndTime();
            }
            measures.addAll(statistics.measures);
        }
        Collections.sort(measures);
        int measuresSize = measures.size();

        recordsPerSecond = (double) measuresSize / (((double) (latestEnd - earliestStart)) / 1000);
        minMeasure = measures.getFirst();
        maxMeasure = measures.getLast();
        medianMeasure = measures.get(measuresSize / 2);
        measureSum = sumMeasures(measures);
        meanMeasure = (double) measureSum / measuresSize;
    }

    private long sumMeasures(List<Long> measures) {
        long sum = 0;
        for (Long measure: measures){
            sum += measure;
        }
        return sum;
    }

    public void writeToFile(String filename){
        try {
            PrintWriter writer = new PrintWriter(filename);
            writer.println("Ilosc rekordow na sekunde - " + String.format( "%.2f", recordsPerSecond));
            writer.println("Sredni czas transmisji rekordu - " + meanMeasure);
            writer.println("Mediana czasu transmisji rekordu - " + medianMeasure);
            writer.println("Najkrotszy czas transmisji rekordu - " + minMeasure);
            writer.println("Najdluzszy czas transmisji rekordu - " + maxMeasure);
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void reset() {
        createdStatistics.clear();
    }

    public List<Statistics> getCreatedStatistics() {
        return createdStatistics;
    }

    public long getMinMeasure() {
        return minMeasure;
    }

    public long getMaxMeasure() {
        return maxMeasure;
    }

    public long getMedianMeasure() {
        return medianMeasure;
    }

    public long getMeasureSum() {
        return measureSum;
    }

    public double getMeanMeasure() {
        return meanMeasure;
    }

    public double getRecordsPerSecond() {
        return recordsPerSecond;
    }
}
