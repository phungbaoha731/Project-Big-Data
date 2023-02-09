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

public class ETLPlayerOverviewES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;


    public ETLPlayerOverviewES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save player overview to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user" + ConfigName.PLAYER_OVERVIEW + "/2023-02-08");
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
        int Performance_Gls = Integer.parseInt(row.getString(9));
        int Performance_Ast = Integer.parseInt(row.getString(10));
        int Performance_PK = Integer.parseInt(row.getString(11));
        int Performance_PKatt = Integer.parseInt(row.getString(12));
        int Performance_Touches = Integer.parseInt(row.getString(17));
        double Expected_xG = Double.parseDouble(row.getString(21));
        double Passes_Cmp_percent = Double.parseDouble(row.getString(28));
        int Dribbles_Att = Integer.parseInt(row.getString(31))

        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("PlayerId", PlayerID);
        map.put("Player name", Player);
        map.put("Number", number);
        map.put("Nation", Nation);
        map.put("Position", Pos);
        map.put("Age", age);
        map.put("Minute played", Min);
        map.put("Performance_Gls", Performance_Gls);
        map.put("Performance_Ast", Performance_Ast);
        map.put("Performance_PK", Performance_PK);
        map.put("Performance_PKatt", Performance_PKatt);
        map.put("Performance_Touches", Performance_Touches);
        map.put("Expected_xG", Expected_xG);
        map.put("Passes Cmp percent", Passes_Cmp_percent);
        map.put("Dribbles_Att", Dribbles_Att);

        esStorage.getBulk().add(new IndexRequest()
                .index("max_" + ConfigName.PLAYER_OVERVIEW_INDEX)
                .id(MatchId).source(map));

    }

    public static void main(String[] args){
        ETLPlayerOverviewES etlPlayerOverviewES = new ETLPlayerOverviewES();
        etlPlayerOverviewES.writeToEs();
        esStorage.close();
    }

}
