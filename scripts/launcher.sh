#!/bin/bash

# Change this to your netid
netid=asa106120

# Root directory of your project
PROJDIR=/people/cs/s/sxg122830/TestProj

# Directory where the config file is located on your local system
#CONFIGLOCAL=$HOME/launch/config.txt
CONFIGLOCAL=C:/Users/amin/Desktop/Distributed/DistributedAlgorithm/src/main/resources/config.txt

# Directory your java classes are in
BINDIR=$PROJDIR/bin

# Your main project class
PROG=HelloWorld
#cat $CONFIGLOCAL | sed -e 's/#.*//'
n=0
#cat $CONFIGLOCAL | sed -e "s/#.*//" | sed -e "/^/s*$/d" | This did not work on windows

cat $CONFIGLOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    echo $i
    while [[ $n -lt $i ]]
    do
    	read line
		
    	p=$( echo $line | awk '{ print $1 }' )
		host=$( echo $line | awk '{ print $2 }' )
        port=$( echo $line | awk '{ print $3 }' )
		
		echo $port
		echo $p
	start C:/Users/amin/Desktop/Distributed/DistributedAlgorithm/scripts/run.bat $port $p $host
	
	#start TITLE $p:$port && "ssh $netid@csgrads1.utdallas.edu ssh $netid@$host java -jar DistComp/project1-0.0.1-SNAPSHOT.jar --config.file=DistComp/config.txt --server.port=%1 --node.id=%2 --logging.config=DistComp/logback.xml" 
	
	#start title $p:$port cmd /c "ssh $netid@csgrads1.utdallas.edu ssh $netid@$host java -jar DistComp/project1-0.0.1-SNAPSHOT.jar --config.file=DistComp/config.txt --server.port=%1 --node.id=%2 --logging.config=DistComp/logback.xml"
	#start $p:$port java -jar DistComp/project1-0.0.1-SNAPSHOT.jar --config.file=C:\Users\amin\Desktop\Distributed\project1\src\main\resources\config.txt --server.port=%1 --node.id=%2 --logging.config=C:\Users\amin\Desktop\logback.xml 
	#gnome-terminal -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host java -cp $BINDIR $PROG $p; $SHELL" &

        n=$(( n + 1 ))
    done
)
