#!/bin/sh

# Kill zombies		
echo "Killing zombies..."		
thewalkingdead=`yarn application -list 2>/dev/null | grep -o "application_[0-9]*_[0-9]*"`		
for zombie in $thewalkingdead		
do		
    yarn application -kill $zombie		
done		
kill `jps | grep "AppMaster" | cut -d " " -f 1`		
kill `jps | grep "TransactionManager" | cut -d " " -f 1`		