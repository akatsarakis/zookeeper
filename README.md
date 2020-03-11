# Zookeeper
Zookeeper offers Sequential Consistency through a leader-based protocol. This project implements the Zookeeper Consistency protocol over RDMA in order to evaluate and compare its performance.

## Optimizations
The protocol is implemented over UD Sends and Receives.
Leader broadcasts using the RDMA Multicast (but there is a knob to revert to unicasts too).
All messages are batched, the stats will print out the batching size of all messages.
The configuration parameters can be tuned in include/zookeeper/main.h


## Repository Contains
1. A modified version of MICA that serves as the store for the Zookeeper
2. A layer that implements the protocol that runs over 1

## How to run
By running the script src/zookeeper/run-zookeeper.sh
The script has a list of ips. These must be replaced with the IPs of your own configuration.
Crucially, the machine that is first in the last is the machine with id 0.
That machine must run first before all the others: because it will be used as the central point to connect and exchange QP info
The IP of that machine must also be replaced in the ZK_REGISTRY_IP variable inside the same script.
Note that the QP setting up is done using memcached, identically to [Herd](https://github.com/efficient/rdma_bench).
That dependency is why the server that acts as the memcahed server must be run first.

Also note that the script passes the name of the device as an input. You must modify this according to what you can see in ibv_devinfo.
This helps disambiguate when servers have multiple NICs on them.

### Dependencies
1. numactl
1. libgsl0-dev
1. libnuma-dev
1. libatmomic_ops
1. libmemcached-dev
1. MLNX_OFED_LINUX-4.1-1.0.2.0

### Settings
1. Run subnet-manager in one of the nodes: '/etc/init.d/opensmd start'
1. On every node apply the following:
 1. echo 8192 | tee /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages > /dev/null
 1. echo 10000000001 | tee /proc/sys/kernel/shmmax /proc/sys/kernel/shmall > /dev/null
 * Make sure that the changes have been applied using cat on the above files
 * The following changes are temporary (i.e. need to be performed after a reboot)

## Tested on
* Infiniband cluster of 9 inter-connected nodes, via a Mellanox MSX6012F-BS switch, each one equiped with a single-port 56Gb Infiniband NIC (Mellanox MCX455A-FCAT PCIe-gen3 x16).
* OS: Ubuntu 14.04 (Kernel: 3.13.0-32-generic) 
