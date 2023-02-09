package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlayerOverviewES {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public PlayerOverviewES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save result matches to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" + ConfigName.PLAYER_OVERVIEW + "/2023-02-08");
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
        String matchId = row.getString(1);

        try {
            map.put("Tournament", row.getString(0));
            map.put("PlayerID", row.getString(2));
            map.put("Player", row.getString(3));
            map.put("#", row.getInt(4));
            map.put("Nation", row.getString(5));
            map.put("Pos", row.getInt(6));
            map.put("Age", Integer.parseInt(row.getString(7).split("-")[0]));
            map.put("Min", row.getInt(8));
            map.put("Att", row.getInt(9));
            map.put("PerformanceGls", row.getInt(10));
            map.put("PerformanceAst", row.getInt(11));
            map.put("PerformancePK", row.getInt(12));
            map.put("PerformancePKatt", row.getInt(13));
            map.put("PerformanceSh", row.getInt(14));
            map.put("PerformanceSoT", row.getInt(15));
            map.put("PerformanceCrdY", row.getInt(16));
            map.put("PerformanceCrdR", row.getInt(17));
            map.put("PerformanceTouches", row.getInt(18));
            map.put("PerformanceTkl", row.getInt(19));
            map.put("PerformanceInt", row.getInt(20));
            map.put("PerformanceBlocks", row.getDouble(21));

        } catch (Exception e){
            e.printStackTrace();
            return;
        }

        esStorage.getBulk().add(new IndexRequest()
                .index(ConfigName.PLAYER_OVERVIEW_INDEX)
                .id(matchId).source(map));
    }

    public static void main(String[] args){
        PlayerOverviewES PlayerOverviewES = new PlayerOverviewES();
        PlayerOverviewES.writeToEs();
        esStorage.close();
    }
}
