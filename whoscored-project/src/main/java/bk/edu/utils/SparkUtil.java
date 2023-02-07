package bk.edu.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Spark instance
 */
public class SparkUtil implements Serializable {
    private SparkSession sparkSession = null;
    private JavaSparkContext sparkContext = null;
    private FileSystem fileSystem = null;

    public SparkUtil(String projectName, String taskName, String master) {

        String appName = String.format("%s: %s", projectName, taskName);

        // disable spark log
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.ERROR);

        this.sparkSession = SparkSession.builder().appName(appName)
                .master(master)
                .config("spark.speculation", "true")
                .config("spark.sql.parquet.binaryAsString", "true")
                .config("spark.hadoop.validateOutputSpecs", "false")
                .config("spark.driver.memory", "1g")
                .config("spark.speculation","false")    // đè hết config set khi chạy
                .config("spark.yarn.access.hadoopFileSystems","/data/raw/day/")
                .getOrCreate();

        this.sparkContext = new JavaSparkContext(this.sparkSession.sparkContext());

    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

}
