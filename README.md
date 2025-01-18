# Commandes à exécuter

## build l'image docker : 
docker buildx build -t hadoop-tp3-img ./deploy

## docker run :
docker run -p 8080:8080 -p 9870:9870 -p 9864:9864 --rm -v "C:\Users\ROUBAUD Mathieu\Documents\EPF\5A\Outils Big Data\hadoop-tp3\data:/tmp/data" -v "C:\Users\ROUBAUD Mathieu\Documents\EPF\5A\Outils Big Data\hadoop-tp3\jars:/tmp/jars" --name hadoop-tp3-cont hadoop-tp3-img

docker exec -it hadoop-tp3-cont su - epfuser

## Commandes Hadoop :
Création du dossier /data :
hadoop fs -mkdir /data

### Copie des datas dans Hadoop :
hadoop fs -put /tmp/data/relationships/ /data/

### Exécution des jobs :
hadoop jar /tmp/jars/tpfinal-mathieu-roubaud-job1.jar /data/relationships/data.txt /data/output/

hadoop jar /tmp/jars/tpfinal-mathieu-roubaud-job2.jar /data/output/ /data/output2/

hadoop jar /tmp/jars/tpfinal-mathieu-roubaud-job3.jar /data/output2/ /data/output3/

### Lecture des fichiers outputs :
#### job 1 :
hdfs dfs -cat /data/output/part-r-00000
hdfs dfs -cat /data/output/part-r-00001
#### job 2 :
hdfs dfs -cat /data/output2/part-r-00000
hdfs dfs -cat /data/output2/part-r-00001
#### job 3 :
hdfs dfs -cat /data/output3/part-r-00000
