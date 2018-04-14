TITLE %2:%1
REM The first line is for DC servers and second is for local. Uncomment the one you like!
ssh asa106120@csgrads1.utdallas.edu ssh -o StrictHostKeyChecking=no asa106120@%3 java -jar DistComp/project1-0.0.1-SNAPSHOT.jar --config.file=DistComp/config.txt --server.port=%1 --node.id=%2 --logging.config=DistComp/logback.xml --wait.time.seconds=20
REM java -jar C:\Users\amin\Desktop\Distributed\project1\target\project1-0.0.1-SNAPSHOT.jar --config.file=C:\Users\amin\Desktop\Distributed\project1\src\main\resources\config.txt --server.port=%1 --node.id=%2 --logging.config=C:\Users\amin\Desktop\logback.xml 
