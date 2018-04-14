#!/bin/bash


# Change this to your netid
netid=sxg122830

#
# Root directory of your project
PROJDIR=$HOME/TestProj

#
# Directory where the config file is located on your local system
CONFIGLOCAL=C:/Users/amin/Desktop/Distributed/project1/src/main/resources/config.txt

n=0

cat $CONFIGLOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    echo $i
    while [[ $n -lt $i ]]
    do
    	read line
        host=$( echo $line | awk '{ print $2 }' )

        echo $host
		start C:/Users/amin/Desktop/Distributed/project1/scripts/end.bat $host
		#gnome-terminal -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host killall -u $netid" &
        sleep 1

        n=$(( n + 1 ))
    done
   
)


echo "Cleanup complete"
