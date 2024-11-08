# ratings_wsb

1) git clone https://github.com/aleksanderbuczek/hadoop
2) ssh sshuser@pac-ssh.azurehdinsight.net "mkdir -p /home/sshuser/pietrusiak/"
3) scp -r  C:/Users/vdi-terminal/hadoop/example_1* sshuser@pac-ssh.azurehdinsight.net:/home/sshuser/pietrusiak/
4)  ssh sshuser@pac-ssh.azurehdinsight.net
5)  hdfs dfs -copyFromLocal /home/sshuser/pietrusiak/example_1/data.txt /user/data-pietrusiak.txt

**zad 3**
6)  hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-files ./pietrusiak/avg_movie_title.py,./pietrusiak/movies.csv \
-mapper "python3 avg_movie_title.py --mapper --movies movies.csv" \
-reducer "python3 avg_movie_title.py --reducer" \
-input /user/ratings.csv \
-output /user/output-avg-movie-title

**zad 2**
7) hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-files ./pietrusiak/avg_ratings.py,./pietrusiak/movies.csv \
-mapper "python3 avg_ratings.py --mapper --movies movies.csv" \
-reducer "python3 avg_ratings.py --reducer" \
-input /user/ratings.csv \
-output /user/output-avg-ratings

9) hdfs dfs -text /user/count-pietrusiak/part-00000 
