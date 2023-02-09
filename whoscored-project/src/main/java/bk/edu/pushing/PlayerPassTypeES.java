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

public class PlayerPassTypeES {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public PlayerPassTypeES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save result matches to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" + ConfigName.PLAYER_PASS_TYPE + "/2023-02-08");
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
            map.put("PassTypeLive", row.getInt(10));
            map.put("PassTypeDead", row.getInt(11));
            map.put("PassTypeFK", row.getInt(12));
            map.put("PassTypeTB", row.getInt(13));
            map.put("PassTypeSw", row.getInt(14));
            map.put("PassTypeCrs", row.getInt(15));
            map.put("PassTypeTI", row.getInt(16));
            map.put("PassTypeCK", row.getInt(17));
            map.put("CornerKicksIn", row.getInt(18));
            map.put("CornerKicksOut", row.getInt(19));
            map.put("CornerKicksStr", row.getInt(20));
            map.put("OutcomesCmp", row.getInt(21));
            map.put("OutcomesOff", row.getInt(22));
            map.put("OutcomesBlocks", row.getInt(23));

        } catch (Exception e){
            e.printStackTrace();
            Thread.currentThread().interrupt();
            return;
        }

        esStorage.getBulk().add(new IndexRequest()
                .index(ConfigName.PLAYER_PASS_TYPE_INDEX)
                .id(matchId).source(map));
    }

    public static void main(String[] args){
        PlayerPassTypeES PlayerPassTypeES = new PlayerPassTypeES();
        PlayerPassTypeES.writeToEs();
        esStorage.close();
    }
}
