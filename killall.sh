#!/bin/sh

# Kill zombies
echo "Killing zombies..."
thewalkingdead=`yarn application -list 2>/dev/null | grep -o "application_[0-9]*_[0-9]*"`
for zombie in $thewalkingdead
do
    yarn application -kill $zombie
done

appMasterPID=`jps | grep "AppMaster" | cut -d " " -f 1`
if [ -n "${appMasterPID}" ];
then
	kill -9 ${appMasterPID}
	echo "AppMaster killed. " ${appMasterPID}
fi

transactionManagerPID=`jps | grep "TransactionManager" | cut -d " " -f 1`
if [ -n "${transactionManagerPID}" ];
then
	kill -9 ${transactionManagerPID}
	echo "TransactionManager killed. " ${transactionManagerPID}
fi
