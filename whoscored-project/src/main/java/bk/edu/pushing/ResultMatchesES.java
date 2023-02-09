package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;
import scala.Serializable;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultMatchesES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public ResultMatchesES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save result matches to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" + ConfigName.RESULT_MATCHES + "/2023-02-08")
                .drop("Round").drop("Notes").drop("_c16").drop("_c17")
                .drop("Wk").drop("Match_Report").drop("Day").drop("xG").drop("xG.1");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            saveResult(listDf.get(i));
            System.out.println("save " + i);
        }
        //spark-submit --master yarn --deploy-mode client --driver-memory 1g --executor-cores 2 --num-executors 1 --executor-memory 2g --class bk.edu.pushing.ETLGkOverViewES whoscored-project-1.0-SNAPSHOT-jar-with-dependencies.jar

    }

    private void saveResult(Row row) {
        Map<String, Object> map = new HashMap<>();
        String matchId = row.getString(0);
        String dateStr = row.getString(1);
        String time = row.getString(2);
        String home = row.getString(3);
        String score = row.getString(4);
        String away = row.getString(5);
        Long date = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
        try {
            date = dateFormat.parse(dateStr).getTime();
        } catch (ParseException e) {
            System.out.println("Date wrong");
            return;
        }
        Integer homeScore;
        Integer awayScore;
        try {
            score = score.replace("?", "-");
            homeScore = Integer.parseInt(score.split("-")[0]);
            awayScore = Integer.parseInt(score.split("-")[1]);
        } catch (Exception e){
            e.printStackTrace();
            Thread.currentThread().interrupt();
            return;
        }
        String attendance = row.getString(6);
        String venue = row.getString(7);
        String reference = row.getString(8);
        map.put("Date", date);
        map.put("Hour", time);
        map.put("Home", home);
        map.put("HomeScore", homeScore);
        map.put("Away", away);
        map.put("AwayScore", awayScore);
        map.put("Attendance", attendance);
        map.put("Venue",venue);
        map.put("Reference",reference);

        esStorage.getBulk().add(new IndexRequest()
                .index(ConfigName.RESULT_MATCHES_INDEX)
                .id(matchId).source(map));
        
    }

    public static void main(String[] args){
        ResultMatchesES resultMatchesEs = new ResultMatchesES();
        resultMatchesEs.writeToEs();
    }
}
