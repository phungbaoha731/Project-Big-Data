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

public class ETLPlayerPassTypeES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public ETLPlayerPassTypeES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save player pass type to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user" + ConfigName.PLAYER_PASSING + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            savePlayerPassType(listDf.get(i));
            System.out.println("save " + i);
        }
        System.out.println("End");
    }

    public void savePlayerPassType(Row row){
        String tournament = row.getString(0);
        String matchId = row.getString(1);
        String playerId = row.getString(2);
        String player = row.getString(3);
        int number = Integer.parseInt(row.getString(4));
        String nation = row.getString(5).split(" ")[1];
        String pos = row.getString(6);
        int age = Integer.parseInt(row.getString(7));
        int min = Integer.parseInt(row.getString(8));
        int att = Integer.parseInt(row.getString(9));
        int passTypesLive = Integer.parseInt(row.getString(10));
        int passTypesDead = Integer.parseInt(row.getString(11));
        int passTypesFK = Integer.parseInt(row.getString(12));
        int passTypesTB = Integer.parseInt(row.getString(13));
        int passTypesSw = Integer.parseInt(row.getString(14));
        int passTypesCrs = Integer.parseInt(row.getString(15));
        int passTypesTI = Integer.parseInt(row.getString(16));
        int passTypesCK = Integer.parseInt(row.getString(17));
        int cornerKicksIn = Integer.parseInt(row.getString(18));
        int cornerKicksOut = Integer.parseInt(row.getString(19));
        int cornerKicksStr = Integer.parseInt(row.getString(20));
        int outcomesCmp = Integer.parseInt(row.getString(21));
        int outcomesOff = Integer.parseInt(row.getString(22));
        int outcomesBlocks = Integer.parseInt(row.getString(23));

        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("PlayerID", playerId);
        map.put("Player", player);
        map.put("Number", number);
        map.put("Nation", nation);
        map.put("Pos", pos);
        map.put("Age", age);
        map.put("Min", min);
        map.put("Att", att);
        map.put("PassTypesLive", passTypesLive);
        map.put("PassTypesDead", passTypesDead);
        map.put("PassTypesFK", passTypesFK);
        map.put("PassTypesTB", passTypesTB);
        map.put("PassTypesSw", passTypesSw);
        map.put("PassTypesCrs", passTypesCrs);
        map.put("PassTypesTI", passTypesTI);
        map.put("PassTypesCK", passTypesCK);
        map.put("CornerKicksIn", cornerKicksIn);
        map.put("CornerKicksOut", cornerKicksOut);
        map.put("CornerKicksStr", cornerKicksStr);
        map.put("OutcomesCmp", outcomesCmp);
        map.put("OutcomesOff", outcomesOff);
        map.put("OutcomesBlocks", outcomesBlocks);

        esStorage.getBulk().add(new IndexRequest()
                .index("max_" + ConfigName.PLAYER_PASS_TYPE_INDEX)
                .id(matchId).source(map));

    }


    public static void main(String[] args){
        ETLPlayerPassTypeES etlPlayerPassTypeES = new ETLPlayerPassTypeES();
        etlPlayerPassTypeES.writeToEs();
        esStorage.close();
    }
}   
