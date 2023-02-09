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

public class ETLPlayerMiscellaneousES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;


    public ETLPlayerMiscellaneousES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save player miscellaneous to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user" + ConfigName.PLAYER_MISCELLANEOUS + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            savePlayerDefensive(listDf.get(i));
            System.out.println("save " + i);
        }
        System.out.println("End");
    }

    public void savePlayerDefensive(Row row){
        String tournament = row.getString(0);
        String MatchId = row.getString(1);
        String PlayerID = row.getString(2);
        String Player = row.getString(3);
        int number = Integer.parseInt(row.getString(4));
        String Nation = row.getString(5);
        String Pos = row.getString(6);
        String age_string = row.getString(7);
        int age = Integer.parseInt(age.split("-", 0)[0]);
        int Min = Integer.parseInt(row.getString(8));
        int Touches_Touches = Integer.parseInt(row.getString(9));
        int Touches_Def_Pen = Integer.parseInt(row.getString(10));
        int Touches_Def_3rd = Integer.parseInt(row.getString(11));
        int Touches_Mid_3rd = Integer.parseInt(row.getString(12));
        int Touches_Att_3rd = Integer.parseInt(row.getString(13));
        int Touches_Att_Pen = Integer.parseInt(row.getString(14));
        int Touches_Live = Integer.parseInt(row.getString(15));
        int Dribbles_Succ = Integer.parseInt(row.getString(16));
        double Dribbles_Succ_percent = Double.parseDouble(row.getString(18));
        int Receiving_Rec = Integer.parseInt(row.getString(21));
        int Receiving_Prog = Integer.parseInt(row.getString(22));

        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("PlayerId", PlayerID);
        map.put("Player name", Player);
        map.put("Number", number);
        map.put("Nation", Nation);
        map.put("Position", Pos);
        map.put("Age", age);
        map.put("Minute played", Min);
        map.put("Touches_Touches", Touches_Touches);
        map.put("Touches_Def_Pen", Touches_Def_Pen);
        map.put("Touches_Def_3rd", Touches_Def_3rd);
        map.put("Touches_Mid_3rd", Touches_Mid_3rd);
        map.put("Touches_Att_3rd", Touches_Att_3rd);
        map.put("Touches_Att_Pen", Touches_Att_Pen);
        map.put("Touches_Live", Touches_Live);
        map.put("Dribbles Succ", Dribbles_Succ);
        map.put("Dribbles Succ percent", Dribbles_Succ_percent);
        map.put("Receiving Rec", Receiving_Rec);
        map.put("Receiving Prog", Receiving_Prog);

        esStorage.getBulk().add(new IndexRequest()
                .index("max_" + ConfigName.PM_INDEX)
                .id(MatchId).source(map));

    }

    public static void main(String[] args){
        ETLPlayerMiscellaneousES etlPlayerMiscellaneousES = new ETLPlayerMiscellaneousES();
        etlPlayerMiscellaneousES.writeToEs();
        esStorage.close();
    }

}
