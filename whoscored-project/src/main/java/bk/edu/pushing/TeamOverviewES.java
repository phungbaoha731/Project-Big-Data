package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.In;
import org.elasticsearch.action.index.IndexRequest;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TeamOverviewES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public TeamOverviewES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save gk teamOverview to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" + ConfigName.TEAM_OVERVIEW + "/2023-02-08")
                .drop("xG").drop("PsxG").drop("Notes")
                .drop("SCA_Event12")
                .drop("SCA_Event14");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            saveTeamOverview(listDf.get(i));
            System.out.println("save " + i);
        }
    }


    private void saveTeamOverview(Row row) {
        Map<String, Object> map = new HashMap<>();
        String matchId = row.getString(1);

        try {
            map.put("Tournament", row.getString(0));
            map.put("Minutes", Integer.parseInt(row.getString(2)));
            map.put("Player", row.getString(3));
            map.put("Squad", row.getString(4));
            map.put("Outcome", row.getString(5));
            map.put("Distance", row.getInt(6));
            map.put("BodyPart", row.getString(7));
            map.put("SCA_Player11", row.getInt(8));
            map.put("SCA_Player13", row.getInt(9));

        } catch (Exception e){
            return;
        }

        esStorage.getBulk().add(new IndexRequest()
                .index(ConfigName.TEAM_OVERVIEW_INDEX)
                .id(matchId).source(map));
    }

    public static void main(String[] args){
        TeamOverviewES teamOverviewES = new TeamOverviewES();
        teamOverviewES.writeToEs();
        esStorage.close();
    }
}
