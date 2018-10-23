#!/usr/bin/env bash

#houston-sanantonio-austin-indianapolis-philly-atlanta-chicago-detroit-baltimore
allIPs=(192.168.5.18 192.168.5.17 192.168.5.19 192.168.5.16 192.168.5.15 192.168.5.11 192.168.5.13)

#houston-sanantonio-philly-austin-indianapolis-chicago-detroit-baltimore-atlanta
#allIPs=(192.168.5.18 192.168.5.17 192.168.5.15 192.168.5.19  129.215.165.6 129.215.165.3 129.215.165.4 129.215.165.2 129.215.165.1)
localIP=$(ip addr | grep 'infiniband' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')

tmp=$((${#localIP}-1))
machine_id=-1

for i in "${!allIPs[@]}"; do
	if [  "${allIPs[i]}" ==  "$localIP" ]; then
		machine_id=$i
	else
    remoteIPs+=( "${allIPs[i]}" )
	fi
done


echo AllIps: "${allIPs[@]}"
echo RemoteIPs: "${remoteIPs[@]}"
echo Machine-Id "$machine_id"


export ZK_REGISTRY_IP="129.215.165.8" # I.E. HOUSTON
export MLX5_SINGLE_THREADED=1
export MLX5_SCATTER_TO_CQE=1

sudo killall memcached
sudo killall zookeeper

# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}


#blue "Removing SHM keys used by the workers 24 -> 24 + Workers_per_machine (request regions hugepages)"
#for i in `seq 0 32`; do
#	key=`expr 24 + $i`
#	sudo ipcrm -M $key 2>/dev/null
#done

# free the  pages workers use

blue "Removing SHM keys used by MICA"
for i in `seq 0 28`; do
	key=`expr 1185 + $i`
	sudo ipcrm -M $key 2>/dev/null
	key=`expr 2185 + $i`
	sudo ipcrm -M $key 2>/dev/null
done

: ${ZK_REGISTRY_IP:?"Need to set ZK_REGISTRY_IP non-empty"}


blue "Removing hugepages"
shm-rm.sh 1>/dev/null 2>/dev/null


blue "Reset server QP registry"
#sudo killall memcached
memcached -l 0.0.0.0 1>/dev/null 2>/dev/null &
sleep 1

blue "Running client and worker threads"
sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	./zookeeper \
	--local-ip $localIP \
	--remote-ips $remoteIPs \
	--machine-id $machine_id \
	--device_name "mlx5_0" \
	2>&1
