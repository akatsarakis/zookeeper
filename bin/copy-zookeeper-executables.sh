#!/usr/bin/env bash
HOSTS=( "houston" "sanantonio")
HOSTS=( "austin" "houston" "sanantonio")
#HOSTS=( "sanantonio" "houston" "philly" "austin")
HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" )
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" "baltimore" "chicago" "atlanta" "detroit")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "chicago" "atlanta" "detroit")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly")
LOCAL_HOST=`hostname`
EXECUTABLES=("zookeeper" "run-zookeeper.sh")
HOME_FOLDER="/home/s1687259/zookeeper/src/zookeeper"
DEST_FOLDER="/home/s1687259/zookeeper-exec/src/zookeeper"
MAKE_FOLDER="/home/s1687259/zookeeper/src"

cd $MAKE_FOLDER
make clean
make
cd -

for EXEC in "${EXECUTABLES[@]}"
do
	#echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
	parallel scp ${HOME_FOLDER}/${EXEC} {}:${DEST_FOLDER}/${EXEC} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done
