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

public class PlayerMiscellaneousES {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public PlayerMiscellaneousES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save result matches to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" + ConfigName.PLAYER_MISCELLANEOUS + "/2023-02-08");
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
            map.put("Touches", row.getInt(9));
            map.put("Touche_Def_Pen", row.getInt(10));
            map.put("Touche_Def_3rd", row.getInt(11));
            map.put("Touche_Mid_3rd", row.getInt(12));
            map.put("Touche_Att_3rd", row.getInt(13));
            map.put("Touche_Att_Pen", row.getInt(14));
            map.put("Touche_Live", row.getInt(15));
            map.put("Dribbles_Suc", row.getInt(16));
            map.put("Dribbles_Att", row.getDouble(17));
            map.put("Dribbles_SucP", row.getDouble(18));
            map.put("Dribbles_Mis", row.getInt(19));
            map.put("Dribbles_Dis", row.getInt(20));
            map.put("Receiving_Rec", row.getInt(21));
            map.put("Receiving_Prog", row.getInt(22));

        } catch (Exception e){
            return;
        }

        esStorage.getBulk().add(new IndexRequest()
                .index(ConfigName.PLAYER_MISCELLANEOUS_INDEX)
                .id(matchId).source(map));
    }

    public static void main(String[] args){
        PlayerMiscellaneousES playerMiscellaneousES = new PlayerMiscellaneousES();
        playerMiscellaneousES.writeToEs();
        esStorage.close();
    }
}
