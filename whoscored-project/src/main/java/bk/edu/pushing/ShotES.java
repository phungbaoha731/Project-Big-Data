package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShotES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public ShotES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save gk teamOverview to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" + ConfigName.SHOT + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            saveShot(listDf.get(i));
            System.out.println("save " + i);
        }
    }

    private void saveShot(Row row) {
        Map<String, Object> map = new HashMap<>();
        String matchId = row.getString(1);

        try {
            map.put("Tournament", row.getString(0));
            map.put("Score", Integer.parseInt(row.getString(2)));
            map.put("Squad", row.getString(3));
            map.put("Possession", Integer.parseInt(row.getString(4).replace("%", "")));
            String[] passingAcc = row.getString(5).split("\\?");
            if(passingAcc[1].contains("%")){
                map.put("PassingAccuracy",Integer.parseInt(passingAcc[1].replace("%", "")));
            } else {
                map.put("PassingAccuracy",Integer.parseInt(passingAcc[2].replace("%", "")));
            }

            String[] shotOnTargets = row.getString(6).split("\\?");
            if(shotOnTargets[1].contains("%")){
                map.put("ShotOnTargets",Integer.parseInt(shotOnTargets[1].replace("%", "")));
            } else {
                map.put("ShotOnTargets",Integer.parseInt(shotOnTargets[2].replace("%", "")));
            }

            String[] saves = row.getString(7).split("\\?");
            if(saves[1].contains("%")){
                map.put("Saves",Integer.parseInt(saves[1].replace("%", "").trim()));
            } else {
                map.put("Saves",Integer.parseInt(saves[0].replace("%", "").trim()));
            }
            String[] cards = row.getString(8).replace("{'Yellow Card':", "")
                    .replace( "'Red Card':", "")
                    .replace("}", "").split(",");
            map.put("YellowCards", Integer.parseInt(cards[0].trim()));
            map.put("RedCards", Integer.parseInt(cards[1].trim()));
            map.put("Fouls", row.getInt(9));
            map.put("Corners", row.getInt(10));
            map.put("Crosses", row.getInt(11));
            map.put("Touches", row.getInt(12));
            map.put("Tackles", row.getInt(13));
            map.put("Interceptions", row.getInt(14));
            map.put("Aerials_Won", row.getInt(15));
            map.put("Clearances", row.getInt(16));
            map.put("Offsides", row.getInt(17));
            map.put("Goal_Kicks", row.getInt(18));
            map.put("Throw_Ins", row.getInt(19));
            map.put("Long_Balls", row.getInt(20));

        } catch (Exception e){
            e.printStackTrace();
            return;
        }

        esStorage.getBulk().add(new IndexRequest()
                .index(ConfigName.SHOT_INDEX)
                .id(matchId).source(map));
    }

    public static void main(String args[]){
        ShotES shotES = new ShotES();
        shotES.writeToEs();
        esStorage.close();
    }

}
