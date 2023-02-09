package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.model.StatsInt;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlayerDefensiveEs implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public PlayerDefensiveEs(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save gk overview to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/max" + ConfigName.PLAYER_DEFENSIVE + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            saveMaxDefensive(listDf.get(i));
            System.out.println("save " + i);
        }
        listDf.clear();
        //spark-submit --master yarn --deploy-mode client --driver-memory 1g --executor-cores 2 --num-executors 1 --executor-memory 2g --class bk.edu.pushing.ETLGkOverViewES whoscored-project-1.0-SNAPSHOT-jar-with-dependencies.jar

    }

    public void saveMaxDefensive(Row row){
        String MatchId = row.getString(0);
        String tournament = row.getString(1);
        String dateStr = row.getString(2);
        Long date = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
        try {
            date = dateFormat.parse(dateStr).getTime();
        } catch (ParseException e) {
            System.out.println("Date wrong");
        }
        if(date == null){
            return;
        }

        String home = row.getString(3);
        String away = row.getString(4);
        String score = row.getString(5).replace("?", "-");

        Row maxShotStoppingSoTA = row.getStruct(6);
        List<String> playerStoppingSoTa = JavaConverters.seqAsJavaList(maxShotStoppingSoTA.getSeq(0));
        int maxShot = maxShotStoppingSoTA.getInt(1);
        StatsInt statsStoppingSota = new StatsInt(playerStoppingSoTa, maxShot);

        Row maxLaunchedCmp = row.getStruct(11);
        List<String> playerLaunchedCmp = JavaConverters.seqAsJavaList(maxLaunchedCmp.getSeq(0));
        int maxLaunchedCmpInt = maxLaunchedCmp.getInt(1);
        StatsInt statsLaunchedCmp = new StatsInt(playerLaunchedCmp, maxLaunchedCmpInt);

        Row maxLaunchedAtt = row.getStruct(12);
        List<String> playerLaunchedAtt = JavaConverters.seqAsJavaList(maxLaunchedAtt.getSeq(0));
        int maxLaunchedAttInt = (int) maxLaunchedAtt.getDouble(1);
        StatsInt statsLaunchedAtt = new StatsInt(playerLaunchedAtt, maxLaunchedAttInt);

        Row maxPassesAtt = row.getStruct(14);
        List<String> playerPassesAtt = JavaConverters.seqAsJavaList(maxPassesAtt.getSeq(0));
        int maxPassesAttInt = maxPassesAtt.getInt(1);
        StatsInt statsPassesAtt = new StatsInt(playerPassesAtt, maxPassesAttInt);

        Row maxGoalKicksAtt = row.getStruct(18);
        List<String> playerGoalKicksAtt = JavaConverters.seqAsJavaList(maxGoalKicksAtt.getSeq(0));
        int maxGoalKicksAttInt = (int)maxGoalKicksAtt.getDouble(1);
        StatsInt statsGoalKicksAtt = new StatsInt(playerGoalKicksAtt, maxGoalKicksAttInt);

        Row maxCrossedOppAtt = row.getStruct(20);
        List<String> playerCrossedOppAtt = JavaConverters.seqAsJavaList(maxCrossedOppAtt.getSeq(0));
        int maxCrossedOppAttInt = maxCrossedOppAtt.getInt(1);
        StatsInt statsCrossedOppAtt = new StatsInt(playerCrossedOppAtt, maxCrossedOppAttInt);

        ObjectMapper oMapper = new ObjectMapper();
        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("Date", date);
        map.put("Home", home);
        map.put("Away", away);
        map.put("Score", score);
        map.put("ShotStoppingSoTA", oMapper.convertValue(statsStoppingSota, Map.class));
        map.put("PassesAtt", oMapper.convertValue(statsPassesAtt, Map.class));
        map.put("LaunchedCmp", oMapper.convertValue(statsLaunchedCmp, Map.class));
        map.put("LaunchedAtt", oMapper.convertValue(statsLaunchedAtt, Map.class));
        map.put("GoalKicksAtt", oMapper.convertValue(statsGoalKicksAtt, Map.class));
        map.put("CrossesOpp", oMapper.convertValue(statsCrossedOppAtt, Map.class));

        esStorage.getBulk().add(new IndexRequest()
                .index("max_" + ConfigName.GK_LOG_INDEX)
                .id(MatchId).source(map));

    }

    private void saveResult(Row row) {

    }

    public static void main(String[] args){
        PlayerDefensiveEs playerDefensiveEs = new PlayerDefensiveEs();
        playerDefensiveEs.writeToEs();
        esStorage.close();
    }
}
