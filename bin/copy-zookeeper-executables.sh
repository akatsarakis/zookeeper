#!/usr/bin/env bash
HOSTS=( "houston" "sanantonio")
HOSTS=( "austin" "houston" "sanantonio")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" )
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" "baltimore" "chicago" "atlanta" "detroit")
LOCAL_HOST=`hostname`
EXECUTABLES=("zookeeper" "run-zookeeper.sh")
HOME_FOLDER="/home/user/zookeeper/src/zookeeper"
DEST_FOLDER="/home/user/zookeeper-exec/src/zookeeper"

cd $HOME_FOLDER
make
cd -

for EXEC in "${EXECUTABLES[@]}"
do
	#echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
	parallel scp ${HOME_FOLDER}/${EXEC} {}:${DEST_FOLDER}/${EXEC} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done
