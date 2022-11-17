# Project-Big-Data

### Hướng dẫn cài đặt:

B1. Clone repo về.<br>
B2. Nếu chưa có cụm hadoop spark. Cài đặt docker và chạy cụm hadoop theo link sau <a>https://github.com/s1mplecc/spark-hadoop-docker</a> <br>
B3. ``` cd whoscored-project ``` <br>
B4. đóng gói ra file jar để chạy spark job: ```mvn package1```
B5. đưa file jar được đóng gói lên cụm chạy spark: example: <br>
```
docker cp ../Project-Big-Data/whoscored-project/target/whoscored-project-1.0-SNAPSHOT-jar-with-dependencies.jar spark-hadoop-docker_spark-worker-2_1:/opt/bitnami/spark
```
<br>
B6. Đưa file Data.csv lên các máy node chạy job: <br>
```   
docker cp /home/nguyenlt/Downloads/Data.csv spark-hadoop-docker_spark-worker-2_1:/
```
<br>
B7: submit spark-job:
<br>
```
./bin/spark-submit --master yarn --deploy-mode client --class bk.edu.first.FirstPush whoscored-project-1.0-SNAPSHOT-jar-with-dependencies.jar
```
