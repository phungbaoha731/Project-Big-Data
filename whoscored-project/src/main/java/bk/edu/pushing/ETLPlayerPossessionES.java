package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.model.StatsInt;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import bk.edu.utils.TimeUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;
import scala.Serializable;
import scala.collection.JavaConverters;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ETLPlayerPossessionES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public ETLPlayerPossessionES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save player possession to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user" + ConfigName.PLAYER_POSSESSION + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            savePlayerPossession(listDf.get(i));
            System.out.println("save " + i);
        }
        System.out.println("End");
    }

    public void savePlayerPossession(Row row){
        String tournament = row.getString(0);
        String matchId = row.getString(1);
        String playerId = row.getString(2);
        String player = row.getString(3);
        int number = Integer.parseInt(row.getString(4));
        String nation = row.getString(5).split(" ")[1];
        String pos = row.getString(6);
        int age = Integer.parseInt(row.getString(7));
        int min = Integer.parseInt(row.getString(8));
        int touches = Integer.parseInt(row.getString(9));
        int touchesDefPen = Integer.parseInt(row.getString(10));    
        int touchesDef3rd = Integer.parseInt(row.getString(11));
        int touchesMid3rd = Integer.parseInt(row.getString(12));
        int touchesAtt3rd = Integer.parseInt(row.getString(13));
        int touchesAttPen = Integer.parseInt(row.getString(14));
        int touchesLive = Integer.parseInt(row.getString(15));
        int dribblesSucc = Integer.parseInt(row.getString(16));
        int dribblesAtt = Integer.parseInt(row.getString(17));
        double dribblesSuccPercent = Double.parseDouble(row.getString(18));
        int dribblesMis = Integer.parseInt(row.getString(19));
        int dribblesDis = Integer.parseInt(row.getString(20));
        int receivingRec = Integer.parseInt(row.getString(21));
        int receivingProg = Integer.parseInt(row.getString(22));

        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("PlayerID", playerId);
        map.put("Player", player);
        map.put("Number", number);
        map.put("Nation", nation);
        map.put("Pos", pos);
        map.put("Age", age);
        map.put("Min", min);
        map.put("Touches", touches);
        map.put("TouchesDefPen", touchesDefPen);
        map.put("TouchesDef3rd", touchesDef3rd);
        map.put("TouchesMid3rd", touchesMid3rd);
        map.put("TouchesAtt3rd", touchesAtt3rd);
        map.put("TouchesAttPen", touchesAttPen);
        map.put("TouchesLive", touchesLive);
        map.put("DribblesSucc", dribblesSucc);
        map.put("DribblesAtt", dribblesAtt);
        map.put("DribblesSuccPercent", dribblesSuccPercent);
        map.put("DribblesMis", dribblesMis);
        map.put("DribblesDis", dribblesDis);
        map.put("ReceivingRec", receivingRec);
        map.put("ReceivingProg", receivingProg);

        esStorage.getBulk().add(new IndexRequest()
                .index("max_" + ConfigName.PLAYER_POSSESSION_INDEX)
                .id(matchId).source(map));

    }


    public static void main(String[] args){
        ETLPlayerPossessionES etlPlayerPossession = new ETLPlayerPossessionES();
        etlPlayerPossession.writeToEs();
        esStorage.close();
    }
}   
