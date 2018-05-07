#ifndef ARMONIA_MAIN_H
#define ARMONIA_MAIN_H

#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>
#include "city.h"
#include "hrd.h"
//-------------------------------------------
/* ----------SYSTEM------------------------ */
//-------------------------------------------
#define TOTAL_CORES 40
#define TOTAL_CORES_ (TOTAL_CORES - 1)
#define SOCKET_NUM 2
#define PHYSICAL_CORES_PER_SOCKET 10
#define PHYSICAL_CORE_DISTANCE 4 // distance between two physical cores of the same socket
#define VIRTUAL_CORES_PER_SOCKET 20
#define WORKER_HYPERTHREADING 1
#define MAX_SERVER_PORTS 1 // better not change that


#define FOLLOWERS_PER_MACHINE 4
#define LEADERS_PER_MACHINE (FOLLOWERS_PER_MACHINE)
#define MACHINE_NUM 2
#define FOLLOWER_MACHINE_NUM (MACHINE_NUM - 1)
#define LEADER_MACHINE 0 // which machine is the leader

#define CACHE_SOCKET (FOLLOWERS_PER_MACHINE < 8 ? 0 : 1 )// socket where the cache is bind

#define CLIENT_NUM (LEADERS_PER_MACHINE * MACHINE_NUM)
#define FOLLOWER_NUM (FOLLOWERS_PER_MACHINE * FOLLOWER_MACHINE_NUM)

#define FOLLOWER_QP_NUM 3 /* The number of QPs for the follower */
#define REMOTE_UD_QP_ID 0 /* The id of the UD QP the clients use for remote reqs */
#define BROADCAST_UD_QP_ID 1 /* The id of the UD QP the clients use for braodcasting */
#define FC_UD_QP_ID 2 /* The id of the UD QP the clients use for flow control */





#define ENABLE_WORKERS_CRCW 1
#define ENABLE_STATIC_LOCAL_ALLOCATION 1 // in crcw statically allocate clients to workers for the local requests
#define DISABLE_LOCALS 1
#define ENABLE_LOCAL_WORKERS_ 0 // this seems to help
#define ENABLE_LOCAL_WORKERS ((ENABLE_WORKERS_CRCW == 1 && DISABLE_LOCALS == 0) ? ENABLE_LOCAL_WORKERS_ : 0)
#define LOCAL_WORKERS 1 // number of workers that are only spawned for local requests
#define ACTIVE_WORKERS_PER_MACHINE ((ENABLE_LOCAL_WORKERS == 1) && (DISABLE_LOCALS == 0) ? (FOLLOWERS_PER_MACHINE - LOCAL_WORKERS) : FOLLOWERS_PER_MACHINE)
#define ENABLE_HUGE_PAGES_FOR_WORKER_REQUEST_REGION 0 // it appears enabling this brings some inconsistencies in performance

#define ENABLE_CACHE_STATS 0
#define EXIT_ON_PRINT 0
#define PRINT_NUM 4
#define DUMP_STATS_2_FILE 0


/*
 * The polling logic in HERD requires the following:
 * 1. 0 < MICA_OP_GET < MICA_OP_PUT < HERD_OP_GET < HERD_OP_PUT
 * 2. HERD_OP_GET = MICA_OP_GET + HERD_MICA_OFFSET
 * 3. HERD_OP_PUT = MICA_OP_PUT + HERD_MICA_OFFSET
 *
 * This allows us to detect HERD requests by checking if the request region
 * opcode is more than MICA_OP_PUT. And then we can convert a HERD opcode to
 * a MICA opcode by subtracting HERD_MICA_OFFSET from it.
 */
#define HERD_MICA_OFFSET 10
#define HERD_OP_GET (MICA_OP_GET + HERD_MICA_OFFSET)
#define HERD_OP_PUT (MICA_OP_PUT + HERD_MICA_OFFSET)


/*-------------------------------------------------
	-----------------PROTOCOLS-----------------
--------------------------------------------------*/
#define FOLLOWER 1
#define LEADER 2
#define ENABLE_MULTIPLE_SESSIONS 1
#define SESSIONS_PER_THREAD 10


/*-------------------------------------------------
	-----------------LEADER------------------------
--------------------------------------------------*/





/*-------------------------------------------------
	-----------------BATCHING ABILITIES-----------------
--------------------------------------------------*/
//-----CLIENT-------

#define ENABLE_THREAD_PARTITIONING_C_TO_W_ 1
#define ENABLE_THREAD_PARTITIONING_C_TO_W (ENABLE_WORKERS_CRCW == 1 ? ENABLE_THREAD_PARTITIONING_C_TO_W_ : 0)
#define BALANCE_REQS_ 0 //
#define BALANCE_REQS  (((ENABLE_WORKERS_CRCW == 1) && (ENABLE_THREAD_PARTITIONING_C_TO_W == 0)) ? BALANCE_REQS_ : 0) //

#define WINDOW_SIZE 256 /* Maximum remote batch*/
#define LOCAL_WINDOW  66 //12 // 21 for 200
#define LOCAL_REGIONS 3 // number of local regions per client
#define LOCAL_REGION_SIZE (LOCAL_WINDOW / LOCAL_REGIONS)
#define WS_PER_WORKER (ENABLE_THREAD_PARTITIONING_C_TO_W == 1 ? 22 : 20) //22 /* Number of outstanding requests kept by each client of any given worker*/
#define MAX_OUTSTANDING_REQS (WS_PER_WORKER * (FOLLOWER_NUM - FOLLOWERS_PER_MACHINE))
#define ENABLE_MULTI_BATCHES 0 // allow multiple batches
#define MAX_REMOTE_RECV_WCS (ENABLE_MULTI_BATCHES == 1 ? (MAX(MAX_OUTSTANDING_REQS, WINDOW_SIZE)) : WINDOW_SIZE)
#define MINIMUM_BATCH_ABILITY 16
#define MIN_EMPTY_PERCENTAGE 5
#define FINISH_BATCH_ON_MISSING_CREDIT 0

//----- WORKER BUFFER
#define WORKER_REQ_SIZE (ENABLE_COALESCING == 1 ? (UD_REQ_SIZE + EXTRA_WORKER_REQ_BYTES) : UD_REQ_SIZE)
#define WORKER_NET_REQ_SIZE (WORKER_REQ_SIZE - GRH_SIZE)
#define MULTIGET_AVAILABLE_SIZE WORKER_NET_REQ_SIZE
#define MAX_COALESCE_PER_MACH ((MULTIGET_AVAILABLE_SIZE - 1) / HERD_GET_REQ_SIZE) // -1 because we overload val_len with the number of gets
#define ENABLE_INLINE_GET_REQS (ENABLE_COALESCING == 1 ? 1 : 1) // Inline get requests even though big objects are used
#define MAXIMUM_INLINE_SIZE 188

//-----WORKER-------
#define WORKER_MAX_BATCH 127
#define ENABLE_MINIMUM_WORKER_BATCHING 0
#define WORKER_MINIMUM_BATCH 16 // DOES NOT WORK


#define WORKER_SEND_BUFF_SIZE ( KEY_SIZE + 1 + 1 + WRKR_COALESCING_BUF_SLOT_SIZE)
#define CLIENT_REMOTE_BUFF_SIZE (GRH_SIZE + WORKER_SEND_BUFF_SIZE)


// INLINING
#define LEADER_ENABLE_INLINING (((USE_BIG_OBJECTS == 1) || (MULTIGET_AVAILABLE_SIZE > MAXIMUM_INLINE_SIZE)) ?  0 : 1)
#define WORKER_RESPONSE_MAX_SIZE (ENABLE_WORKER_COALESCING == 1 ? (MAX_COALESCE_PER_MACH * HERD_VALUE_SIZE) : HERD_VALUE_SIZE)
#define WORKER_ENABLE_INLINING (((USE_BIG_OBJECTS == 1) || (WORKER_RESPONSE_MAX_SIZE > MAXIMUM_INLINE_SIZE)) ?  0 : 1)

// CACHE
#define ENABLE_HOT_KEY_TRACKING 0
#define HOTTEST_KEYS_TO_TRACK 20



/*-------------------------------------------------
-----------------DEBUGGING-------------------------
--------------------------------------------------*/
#define ENABLE_SS_DEBUGGING 0 // first thing to open in a deadlock
#define ENABLE_ASSERTIONS 1
#define ENABLE_STAT_COUNTING 1
#define MEASURE_LATENCY 0
#define REMOTE_LATENCY_MARK 100 // mark a remote request for measurement by attaching this to the imm_data of the wr
#define ENABLE_WINDOW_STATS 0

#define DO_ONLY_LOCALS 0
#define USE_A_SINGLE_KEY 0
#define DISABLE_HYPERTHREADING 0 // do not shcedule two threads on the same core
#define ENABLE_WAKE_UP 0
#define USE_ONLY_BIG_MESSAGES 0 // deprecated
#define ONLY_CACHE_HITS 0
#define DEFAULT_SL 0 //default service level
#define VERBOSE_DEBUG 0
#define STALLING_DEBUG 0 // prints information about the stalled ops, check debug_stalling_LIN()
#define DEBUG_COALESCING 0
#define DEBUG_WORKER_RECVS 0


/*-------------------------------------------------
	-----------------TRACE-----------------
--------------------------------------------------*/
#define SEND_ONLY_TO_ONE_MACHINE 0 // Dynamically alters trace to send all the requests to one machinr
#define SEND_ONLY_TO_NEXT_MACHINE 0 // Dynamically alters trace so each machine sends its requests to the next one
#define BALANCE_REQS_IN_CHUNKS 0
#define CHUNK_NUM 0
#define BALANCE_HOT_REQS 0 // Use a uniform access pattern among hot requests
#define BALANCE_HOT_WRITES 0// Use a uniform access pattern among hot writes
#define ENABLE_HOT_REQ_GROUPING 0 // Group the hot keys, such that the accesses pof key correspond to a group
#define NUM_OF_KEYS_TO_GROUP 10
#define GROUP_SIZE 50
#define SKEW_EXPONENT_A 99 // representation divided by 100 (i.e. 99 means a = 0.99)
#define EMULATING_CREW 1 // emulate crew, to facilitate running the CREW baseline
#define RANDOM_MACHINE 0 // pick a rnadom machine
#define DISABLE_CACHE 0 // Run Baseline
#define LOAD_BALANCE 1 // Use a uniform access pattern
#define EMULATE_SWITCH_KV 0 // Does nothing..
#define SWITCH_KV_NODE 0 // which machine is the cache
#define FOLLOWER_DOES_ONLY_READS 1

/*-------------------------------------------------
	-----------------CONSISTENCY-------------------------
--------------------------------------------------*/
//----MULTICAST
#define ENABLE_MULTICAST 0
#define MULTICAST_TESTING_ 0
#define MULTICAST_TESTING (ENABLE_MULTICAST == 1 ? MULTICAST_TESTING_ : 0)
#define SEND_MCAST_QP 0
#define RECV_MCAST_QP 1
#define MCAST_QPS MACHINE_NUM
#define MCAST_GROUPS_PER_CLIENT MACHINE_NUM

// ------COMMON-------------------
#define MAX_BCAST_BATCH (ENABLE_MULTICAST == 1 ? 4 : 4) //8 //(128 / (MACHINE_NUM - 1)) // how many broadcasts can fit in a batch
#define MESSAGES_IN_BCAST (ENABLE_MULTICAST == 1 ? 1 : (FOLLOWER_MACHINE_NUM))
#define MESSAGES_IN_BCAST_BATCH MAX_BCAST_BATCH * MESSAGES_IN_BCAST //must be smaller than the q_depth
#define BCAST_TO_CACHE_BATCH 90 //100 // helps to keep small //47 for SC

//----------SC flow control-----------------
#define SC_CREDITS 60 //experiments with 33
#define SC_CREDIT_DIVIDER 2 /*This is actually useful in high write ratios TODO tweak this*/
#define SC_CREDITS_IN_MESSAGE (SC_CREDITS / SC_CREDIT_DIVIDER)
#define SC_MAX_CREDIT_WRS (SC_CREDITS / SC_CREDITS_IN_MESSAGE) * (MACHINE_NUM - 1)
#define SC_MAX_COH_MESSAGES (SC_CREDITS * (MACHINE_NUM - 1))
#define SC_MAX_COH_RECEIVES (SC_CREDITS * (MACHINE_NUM - 1))
#define SC_MAX_CREDIT_RECVS (CEILING(SC_MAX_COH_MESSAGES, SC_CREDITS_IN_MESSAGE))
#define SC_VIRTUAL_CHANNELS 1
#define SC_UPD_VC 0


//----------LIN flow control-----------------
#define CREDITS_FOR_EACH_CLIENT 30
#define UPD_CREDITS (CREDITS_FOR_EACH_CLIENT)
#define ACK_CREDITS (CREDITS_FOR_EACH_CLIENT)
#define INV_CREDITS (CREDITS_FOR_EACH_CLIENT)
#define BROADCAST_CREDITS (UPD_CREDITS + ACK_CREDITS + INV_CREDITS) /* Credits for each machine to issue Broadcasts */
#define VIRTUAL_CHANNELS 3 // upds acks and invs
//#define ACK_VC 0
#define INV_VC 1
#define UPD_VC 2
#define LIN_CREDIT_DIVIDER 2 //1 /// this  has the potential to cause deadlocks //  =take care that this can be a big part of the network traffic
#define CREDITS_IN_MESSAGE (CREDITS_FOR_EACH_CLIENT / LIN_CREDIT_DIVIDER) /* How many credits exist in a single back-pressure message- seems to be working with / 3*/
#define MAX_CREDIT_WRS (BROADCAST_CREDITS / CREDITS_IN_MESSAGE) * (MACHINE_NUM - 1)
#define MAX_COH_MESSAGES ((MACHINE_NUM - 1) * BROADCAST_CREDITS)
#define MAX_COH_RECEIVES ((MACHINE_NUM - 1) * BROADCAST_CREDITS)

/* --------------------------------------------------------------------------------
 * -----------------------------ZOOKEEPER---------------------------------------
 * --------------------------------------------------------------------------------
 * --------------------------------------------------------------------------------*/


//--------FOLOWER Flow Control
#define W_CREDITS 15

//--------LEADER Flow Control
#define PREPARE_CREDITS 15
#define COMMIT_CREDITS 15
#define BCAST_CREDITS (PREPARE_CREDITS + COMMIT_CREDITS)
#define LDR_PREPOST_RECEIVES_NUM (W_CREDITS * FOLLOWER_MACHINE_NUM)
#define MAX_OF_CREDITS MAX(W_CREDITS, PREPARE_CREDITS)
#define LDR_MAX_RECEIVE_WRS (FOLLOWER_MACHINE_NUM * MAX_OF_CREDITS)
#define LDR_MAX_RECEIVES (FOLLOWER_MACHINE_NUM * (W_CREDITS + PREPARE_CREDITS))
#define LDR_VIRTUAL_CHANNELS 2 // upds acks and invs
#define LDR_VC_NUM 2
#define PREP_VC 0
#define COMM_VC 1
#define FLR_VC_NUM 2
#define ACK_VC 0
#define W_VC 1
#define LDR_CREDIT_DIVIDER (W_CREDITS)
#define LDR_CREDITS_IN_MESSAGE (W_CREDITS / LDR_CREDIT_DIVIDER)
#define FLR_CREDIT_DIVIDER (LDR_CREDIT_DIVIDER)
#define FLR_CREDITS_IN_MESSAGE (COMMIT_CREDITS / FLR_CREDIT_DIVIDER)

// if this is smaller than MAX_BCAST_BATCH + 2 it will deadlock because the signaling messaged is polled before actually posted
#define COM_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (FOLLOWER_MACHINE_NUM)), (MAX_BCAST_BATCH + 2))
#define PREP_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (FOLLOWER_MACHINE_NUM)), (MAX_BCAST_BATCH + 2))



// -------ACKS-------------
#define LDR_QUORUM_OF_ACKS (FOLLOWER_MACHINE_NUM)
#define MAX_LIDS_IN_AN_ACK K_64_
#define ACK_SIZE 12
//#define MAX_ACK_COALESCE 5
#define COM_ACK_HEADER_SIZE 4 // follower id, opcode, coalesce_num
#define FLR_ACK_SEND_SIZE (12) // a local global id and its metadata
#define LDR_ACK_RECV_SIZE (GRH_SIZE + (FLR_ACK_SEND_SIZE))


// -- COMMITS-----
#define MAX_LIDS_IN_A_COMMIT K_64_
#define COM_SIZE 8 // gid(8)
#define COM_MES_HEADER_SIZE 4 // opcode + coalesce num
//#define MAX_COM_COALESCE 2
#define LDR_COM_SEND_SIZE (COM_SIZE + COM_MES_HEADER_SIZE)
#define FLR_COM_RECV_SIZE (GRH_SIZE + LDR_COM_SEND_SIZE)
#define COM_ENABLE_INLINING ((LDR_COM_SEND_SIZE < MAXIMUM_INLINE_SIZE) ? 1: 0)
#define COMMIT_FIFO_SIZE ((COM_ENABLE_INLINING == 1) ? (COMMIT_CREDITS) : (COM_BCAST_SS_BATCH))

//---WRITES---
#define MAX_W_COALESCE 1
#define WRITE_HEADER (KEY_SIZE + 2) // opcode + val_len
#define SINGLE_WRITE_PAYLOAD (VALUE_SIZE + WRITE_HEADER)
#define WRITE_MESSAGES_VALUE_SIZE (MAX_W_COALESCE * SINGLE_WRITE_PAYLOAD)
#define FLR_W_SEND_SIZE (WRITE_MESSAGES_VALUE_SIZE)
#define LDR_W_RECV_SIZE (GRH_SIZE + FLR_W_SEND_SIZE)
#define FLR_PREPARE_ENABLE_INLINING ((FLR_W_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)

//--PREPARES
#define MAX_PREP_COALESCE 2
#define PREP_MES_HEADER 6 // opcode(1), coalesce_num(1) l_id (4)
#define PREP_SIZE (KEY_SIZE + 2 + VALUE_SIZE) // Size of a write
#define LDR_PREP_SEND_SIZE (PREP_MES_HEADER + (MAX_PREP_COALESCE * PREP_SIZE))
#define FLR_PREP_RECV_SIZE (GRH_SIZE + LDR_PREP_SEND_SIZE)

#define LEADER_PREPARE_ENABLE_INLINING (((USE_BIG_OBJECTS == 1) || (LDR_PREP_SEND_SIZE > MAXIMUM_INLINE_SIZE)) ?  0 : 1)


//---------LEADER-----------------------
// PREP_ACK_QP_ID 0: send Prepares -- receive ACKs
#define LDR_MAX_PREP_WRS (MESSAGES_IN_BCAST_BATCH)
#define LDR_MAX_RECV_ACK_WRS (FOLLOWER_MACHINE_NUM * PREPARE_CREDITS)
// COMMIT_W_QP_ID 1: send Commits  -- receive Writes
#define LDR_MAX_COM_WRS (MESSAGES_IN_BCAST_BATCH)
#define LDR_MAX_RECV_W_WRS (FOLLOWER_MACHINE_NUM * W_CREDITS)
// Credits WRs
#define LDR_MAX_CREDIT_WRS ((W_CREDITS / LDR_CREDITS_IN_MESSAGE ) * FOLLOWER_MACHINE_NUM)
#define LDR_MAX_CREDIT_RECV ((COMMIT_CREDITS / FLR_CREDITS_IN_MESSAGE ) * FOLLOWER_MACHINE_NUM)


//--------FOLLOWER--------------
// // PREP_ACK_QP_ID 0: receive Prepares -- send ACKs
#define FLR_MAX_ACK_WRS (1)
#define FLR_MAX_RECV_PREP_WRS (PREPARE_CREDITS)
// COMMIT_W_QP_ID 1: send Writes  -- receive Commits
#define FLR_MAX_W_WRS (W_CREDITS)
#define FLR_MAX_RECV_COM_WRS (COMMIT_CREDITS)
// Credits WRs
#define FLR_MAX_CREDIT_WRS (COMMIT_CREDITS / FLR_CREDITS_IN_MESSAGE )
#define FLR_MAX_CREDIT_RECV (W_CREDITS / LDR_CREDITS_IN_MESSAGE)
#define ACK_SEND_SS_BATCH MAX(MIN_SS_BATCH, (FLR_MAX_ACK_WRS + 2))

//-- LEADER
#define LEADER_W_BUF_SIZE ((LDR_W_RECV_SIZE * FOLLOWER_MACHINE_NUM) * W_CREDITS)
#define LEADER_ACK_BUF_SIZE (LDR_ACK_RECV_SIZE * FOLLOWER_MACHINE_NUM * PREPARE_CREDITS)
#define LEADER_W_BUF_SLOTS (FOLLOWER_MACHINE_NUM * W_CREDITS)
#define LEADER_ACK_BUF_SLOTS (FOLLOWER_MACHINE_NUM * PREPARE_CREDITS)
#define LEADER_BUF_SIZE (LEADER_W_BUF_SIZE + LEADER_ACK_BUF_SIZE)
#define LEADER_BUF_SLOTS (LEADER_W_BUF_SLOTS + LEADER_ACK_BUF_SLOTS)

#define LEADER_REMOTE_W_SLOTS (FOLLOWER_MACHINE_NUM * W_CREDITS * MAX_W_COALESCE)
#define LEADER_PENDING_WRITES (SESSIONS_PER_THREAD + LEADER_REMOTE_W_SLOTS)
#define PREP_FIFO_SIZE (LEADER_PENDING_WRITES)


//--FOLLOWER
#define FLR_PREP_BUF_SIZE (FLR_PREP_RECV_SIZE  * PREPARE_CREDITS)
#define FLR_COM_BUF_SIZE (FLR_COM_RECV_SIZE * COMMIT_CREDITS)
#define FLR_PREP_BUF_SLOTS (3 * PREPARE_CREDITS)
#define FLR_COM_BUF_SLOTS (COMMIT_CREDITS)
#define FLR_BUF_SIZE (FLR_PREP_BUF_SIZE + FLR_COM_BUF_SIZE)
#define FLR_BUF_SLOTS (FLR_PREP_BUF_SLOTS + FLR_COM_BUF_SLOTS)

#define FLR_PENDING_WRITES (2 * PREPARE_CREDITS * MAX_PREP_COALESCE) // 2/3 of the buffer
#define FLR_DISALLOW_OUT_OF_ORDER_PREPARES 1
/*-------------------------------------------------
-----------------QUEUE DEPTHS-------------------------
--------------------------------------------------*/




#define LEADER_QP_NUM 3 /* The number of QPs for the leader */
#define PREP_ACK_QP_ID 0
#define COMMIT_W_QP_ID 1
#define FC_QP_ID 2

/*
 * -------LEADER-------------
 * 1st Dgram send Prepares -- receive ACKs
 * 2nd Dgram send Commits  -- receive Writes
 * 3rd Dgram send Credits  -- receive Credits
 *
 * ------FOLLOWER-----------
 * 1st Dgram receive prepares -- send Acks
 * 2nd Dgram receive Commits  -- send Writes
 * 3rd Dgram receive Credits  -- send Credits
 * */

// LDR - Receive
#define LDR_RECV_ACK_Q_DEPTH 500 //(LDR_MAX_RECV_ACK_WRS + 3)
#define LDR_RECV_W_Q_DEPTH 500 //(LDR_MAX_RECV_W_WRS + 3)
#define LDR_RECV_CR_Q_DEPTH 500 //()
// LDR - Send
#define LDR_SEND_PREP_Q_DEPTH 500 //(LDR_MAX_RECV_ACK_WRS + 3)
#define LDR_SEND_COM_Q_DEPTH 500 //(LDR_MAX_RECV_W_WRS + 3)
#define LDR_SEND_CR_Q_DEPTH 500 //()

// FLR - Receive
#define FLR_RECV_PREP_Q_DEPTH 500 //
#define FLR_RECV_COM_Q_DEPTH 500 //
#define FLR_RECV_CR_Q_DEPTH 500 //()
// FLR - Send
#define FLR_SEND_ACK_Q_DEPTH 500 //
#define FLR_SEND_W_Q_DEPTH 500 //
#define FLR_SEND_CR_Q_DEPTH 500 //







/* ----------------
 * -------------OLD STUFF
 * ---------------------------*/






//---------Buffer Space-------------
#define LIN_CLT_BUF_SIZE (UD_REQ_SIZE * (MACHINE_NUM - 1) * BROADCAST_CREDITS)
#define SC_CLT_BUF_SIZE (UD_REQ_SIZE * (MACHINE_NUM - 1) * SC_CREDITS)
#define LIN_CLT_BUF_SLOTS ((MACHINE_NUM - 1) * BROADCAST_CREDITS)
#define SC_CLT_BUF_SLOTS (SC_CLT_BUF_SIZE  / UD_REQ_SIZE)

#define OPS_BUFS_NUM (LEADER_ENABLE_INLINING == 1 ? 2 : 3) // how many OPS buffers are in use
//#define EXTENDED_OPS_SIZE (OPS_BUFS_NUM * CACHE_BATCH_SIZE * CACHE_OP_SIZE)
#define COH_BUF_SIZE (LEADER_ENABLE_INLINING == 1 ?	(MAX_BCAST_BATCH * MICA_OP_SIZE) : (BROADCAST_SS_BATCH * MICA_OP_SIZE))
#define COH_BUF_SLOTS (LEADER_ENABLE_INLINING == 1 ? MAX_BCAST_BATCH : BROADCAST_SS_BATCH)
/* We post receives for credits after sending broadcasts or acks,
	For Broadcasts the maximum number is: (MACHINE_NUM - 1) * (CEILING(MAX_BCAST_BATCH, CREDITS_IN_MESSAGE))
	For acks the maximum number is: CEILING(BCAST_TO_CACHE_BATCH, REDITS_IN_MESSAGE)   */
#define MAX_CREDIT_RECVS_FOR_BCASTS (MACHINE_NUM - 1) * (CEILING(MAX_BCAST_BATCH, CREDITS_IN_MESSAGE))
#define MAX_CREDIT_RECVS_FOR_ACKS (CEILING(BCAST_TO_CACHE_BATCH, CREDITS_IN_MESSAGE))
#define MAX_CREDIT_RECVS (MAX(MAX_CREDIT_RECVS_FOR_BCASTS, MAX_CREDIT_RECVS_FOR_ACKS))



/*-------------------------------------------------
-----------------SELECTIVE SIGNALING-------------------------
--------------------------------------------------*/
#define MIN_SS_BATCH 127// THe minimum ss batch
#define CREDIT_SS_BATCH MAX(MIN_SS_BATCH, (MAX_CREDIT_WRS + 1))
#define CREDIT_SS_BATCH_ (CREDIT_SS_BATCH - 1)
#define SC_CREDIT_SS_BATCH MAX(MIN_SS_BATCH, (SC_MAX_CREDIT_WRS + 1))
#define SC_CREDIT_SS_BATCH_ (SC_CREDIT_SS_BATCH - 1)
#define WORKER_SS_BATCH MAX(MIN_SS_BATCH, (WORKER_MAX_BATCH + 1))
#define WORKER_SS_BATCH_ (WORKER_SS_BATCH - 1)
#define CLIENT_SS_BATCH MAX(MIN_SS_BATCH, (WINDOW_SIZE + 1))
#define CLIENT_SS_BATCH_ (CLIENT_SS_BATCH - 1)
// if this is smaller than MAX_BCAST_BATCH + 2 it will deadlock because the signaling messaged is polled before actually posted
#define BROADCAST_SS_BATCH MAX((MIN_SS_BATCH / (MACHINE_NUM - 1)), (MAX_BCAST_BATCH + 2))
#define ACK_SS_BATCH MAX(MIN_SS_BATCH, (BCAST_TO_CACHE_BATCH + 1)) //* (MACHINE_NUM - 1)




//RECV
#define WORKER_RECV_Q_DEPTH  (((MACHINE_NUM - 1) * CEILING(LEADERS_PER_MACHINE, FOLLOWER_QP_NUM) * WS_PER_WORKER) + 3) // + 3 for good measre
#define CLIENT_RECV_REM_Q_DEPTH ((ENABLE_MULTI_BATCHES == 1 ? MAX_OUTSTANDING_REQS :  2 * CLIENT_SS_BATCH) + 3)

#define SC_CLIENT_RECV_BR_Q_DEPTH (SC_MAX_COH_RECEIVES + 3)
#define LIN_CLIENT_RECV_BR_Q_DEPTH (MAX_COH_RECEIVES + 3)

#define SC_CLIENT_RECV_CR_Q_DEPTH (SC_MAX_CREDIT_RECVS + 3) // recv credits SC
#define LIN_CLIENT_RECV_CR_Q_DEPTH (MAX_COH_MESSAGES  + 8) // a reasonable upper bound

// SEND
#define WORKER_SEND_Q_DEPTH  WORKER_MAX_BATCH + 3 // + 3 for good measure
#define CLIENT_SEND_REM_Q_DEPTH  ((ENABLE_MULTI_BATCHES == 1  ? MAX_OUTSTANDING_REQS : CLIENT_SS_BATCH) + 3) // 60)

#define SC_CLIENT_SEND_BR_Q_DEPTH (MAX((MACHINE_NUM - 1) * BROADCAST_SS_BATCH, SC_MAX_COH_MESSAGES + 14) + 3)
#define LIN_CLIENT_SEND_BR_Q_DEPTH (MAX(MAX_COH_MESSAGES, (BROADCAST_SS_BATCH * (MACHINE_NUM - 1) + ACK_SS_BATCH)) + 13)

#define SC_CLIENT_SEND_CR_Q_DEPTH  (2 * SC_CREDIT_SS_BATCH + 3) // send credits SC
#define LIN_CLIENT_SEND_CR_Q_DEPTH (2 * CREDIT_SS_BATCH + 13)

// WORKERS synchronization options
#if ENABLE_WORKERS_CRCW == 1
extern struct mica_kv kv;
# define KVS_BATCH_OP mica_batch_op_crcw
#else /*ENABLE_WORKERS_CRCW == 0*/
# define KVS_BATCH_OP mica_batch_op
#endif
//LATENCY Measurment
#define MAX_LATENCY 400 //in us
#define LATENCY_BUCKETS 200 //latency accuracy

/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24
#define RR_SIZE (16 * 1024 * 1024)	/* Request region size */

#define OFFSET(wn, cn, ws) ((wn * LEADERS_PER_MACHINE * LOCAL_WINDOW) + \
	(cn * LOCAL_WINDOW) + ws) // There was a bug here, wehre Instead of Clients per machine, it was CLIENT_NUM

//Defines for parsing the trace
#define _200_K 200000
#define MAX_TRACE_SIZE _200_K
#define FEED_FROM_TRACE 0
#define TRACE_SIZE K_128
#define NOP 0
#define HOT_WRITE 1
#define HOT_READ 2
#define REMOTE_WRITE 3
#define REMOTE_READ 4
#define LOCAL_WRITE 5
#define LOCAL_READ 6



#define IS_READ(X)  ((X) == HOT_READ || (X) == LOCAL_READ || (X) == REMOTE_READ  ? 1 : 0)
#define IS_WRITE(X)  ((X) == HOT_WRITE || (X) == LOCAL_WRITE || (X) == REMOTE_WRITE  ? 1 : 0)
#define IS_HOT(X)  ((X) == HOT_WRITE || (X) == HOT_READ ? 1 : 0)
#define IS_NORMAL(X)  (!IS_HOT((X)))
#define IS_LOCAL(X) ((X) == LOCAL_WRITE || (X) == LOCAL_READ ? 1 : 0)
#define IS_REMOTE(X) ((X) == REMOTE_WRITE || (X) == REMOTE_READ ? 1 : 0)

struct trace_command {
	uint8_t  opcode;
	uint8_t  home_machine_id;
	uint8_t  home_worker_id;
	uint32_t key_id;
	uint128 key_hash;
};

struct coalesce_inf {
	uint16_t wr_i;
	uint16_t slots;
	uint16_t op_i;
	uint16_t wrkr;
};
/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same cache line */
struct remote_qp {
	struct ibv_ah *ah;
	int qpn;
	// no padding needed- false sharing is not an issue, only fragmentation
};

/*
 *  SENT means we sent the prepare message
 *  READY means all acks have been gathered
 *  SEND_COMMITS mens it has been propagated to the
 *  cache and commits should be sent out
 * */
enum write_state {INVALID, VALID, SENT, READY, SEND_COMMITTS};



struct completed_writes {
  struct write_op **w_ops; // FIFO QUEUE that points to the next write to commit
  enum write_state *w_state;
  uint32_t *p_writes_ptr;
  uint16_t pull_ptr;
  uint16_t push_ptr;

};


// The format of an ack message
struct ack_message {
  uint8_t follower_id;
  uint8_t opcode;
	uint16_t ack_num;
	uint8_t local_id[8]; // the first local id that is being acked
};


struct ack_message_ud_req {
	uint8_t grh[GRH_SIZE];
  struct ack_message ack;

 };


// The format of a commit message
struct com_message {
  uint16_t com_num;
	uint16_t opcode;
	uint8_t l_id[8];
};

// commit message plus the grh
struct com_message_ud_req {
	uint8_t grh[GRH_SIZE];
  struct com_message com;

};

struct prepare {
	uint8_t flr_id;
	uint8_t session_id[3];
	uint8_t g_id[4]; //send the bottom half of the gid
	uint8_t key[8];
	uint8_t opcode; //override opcode
	uint8_t val_len;
	uint8_t value[VALUE_SIZE];
};

// prepare message
struct prep_message {
	uint8_t opcode;
	uint8_t coalesce_num;
	uint8_t l_id[4]; // send the bottom half of the lid
	struct prepare prepare[MAX_PREP_COALESCE];
};

struct prep_message_ud_req {
	uint8_t grh[GRH_SIZE];
	struct prep_message prepare;
};
// The entires in the commit prep_message are distinct batches of commits
struct commit_fifo {
  struct com_message *commits;
  uint16_t push_ptr;
  uint16_t pull_ptr;
  uint32_t size; // number of commits rather than  messages
};

struct fifo {
	void *fifo;
	uint32_t push_ptr;
	uint32_t pull_ptr;
	uint32_t size;

};

struct prep_fifo {
	struct prep_message *prep_message;
	uint32_t push_ptr;
	uint32_t pull_ptr;
	uint32_t bcast_pull_ptr;
	uint32_t bcast_size; // number of prepares not messages!
	uint32_t size;
	uint32_t backward_ptrs[PREP_FIFO_SIZE];

};


// A data structute that keeps track of the outstanding writes
struct pending_writes {
	uint64_t *g_id;
	struct prep_fifo *prep_fifo;
	struct cache_op **ptrs_to_ops;
	uint64_t local_w_id;
	uint32_t *session_id;
	enum write_state *w_state;
	uint32_t push_ptr;
	uint32_t pull_ptr;
	uint32_t prep_pull_ptr; // Where to pull prepares from
	uint32_t size;
	uint32_t unordered_ptr;
	uint8_t *flr_id;
	uint8_t *acks_seen;
	bool *is_local;
	bool *session_has_pending_write;
	bool all_sessions_stalled;
};

// follower pending writes
struct flr_p_writes {
	uint64_t *g_id;
	struct cache_op **ptrs_to_ops;
	uint64_t local_w_id;
	uint32_t *session_id;
	enum write_state *w_state;
	uint32_t push_ptr;
	uint32_t pull_ptr;
	uint32_t size;
	uint8_t *flr_id;
	bool *is_local;
	bool *session_has_pending_write;
	bool all_sessions_stalled;
};

// struct for the follower to keep track of the acks it has sent
struct pending_acks {
	uint32_t slots_ahead;
	uint32_t acks_to_send;

};

struct recv_info {
	uint32_t *push_ptr;
	uint32_t buf_slots;
	uint32_t slot_size;
	uint32_t posted_recvs;
	struct ibv_recv_wr *recv_wr;
	struct ibv_qp * recv_qp;
	struct ibv_sge* recv_sgl;
	void* buf;

};

struct w_message {
  uint8_t flr_id[4];
  uint8_t session_id[4];
  uint8_t key[TRUE_KEY_SIZE];	/* 8B */
  uint8_t opcode;
  uint8_t w_num;
  uint8_t value[VALUE_SIZE];
};

//struct w_message {
//  struct write w[MAX_W_COALESCE];
//};

struct w_message_ud_req {
//  struct ibv_grh grh; // compiler puts padding with this
	uint8_t unused[GRH_SIZE];
  struct w_message writes[MAX_W_COALESCE];
};

struct thread_stats { // 2 cache lines
	long long cache_hits_per_thread;
	long long remotes_per_client;
	long long locals_per_client;

	long long updates_per_client;
	long long acks_per_client;  //only LIN
	long long invs_per_client; //only LIN

	long long received_updates_per_client;
	long long received_acks_per_client; //only LIN
	long long received_invs_per_client; //only LIN

	long long remote_messages_per_client;
	long long cold_keys_per_trace;
	long long batches_per_client;

	long long stalled_time_per_client;

	double empty_reqs_per_trace;
	long long wasted_loops;
	double tot_empty_reqs_per_trace;


	//long long unused[3]; // padding to avoid false sharing
};


struct follower_stats { // 1 cache line
	long long cache_hits_per_thread;
	long long remotes_per_client;
	long long locals_per_client;

	long long updates_per_client;
	long long acks_per_client;  //only LIN
	long long invs_per_client; //only LIN

	long long received_updates_per_client;
	long long received_acks_per_client; //only LIN
	long long received_invs_per_client; //only LIN

	long long remote_messages_per_client;
	long long cold_keys_per_trace;
	long long batches_per_client;

	long long stalled_time_per_client;

	double empty_reqs_per_trace;
	long long wasted_loops;
	double tot_empty_reqs_per_trace;

	long long unused[4]; // padding to avoid false sharing
};


// a client sends to a particular ud qp to all workers, therefore to better utilize its L1 cache
// we store worker AHs by QP instead of by worker id
extern struct remote_qp remote_follower_qp[FOLLOWER_MACHINE_NUM][FOLLOWERS_PER_MACHINE][FOLLOWER_QP_NUM];
extern struct remote_qp remote_leader_qp[LEADERS_PER_MACHINE][LEADER_QP_NUM];
extern atomic_char qps_are_set_up;
extern atomic_char local_recv_flag[FOLLOWERS_PER_MACHINE][LEADERS_PER_MACHINE][64]; //false sharing problem -- fixed with padding
extern struct thread_stats t_stats[LEADERS_PER_MACHINE];
extern struct follower_stats f_stats[FOLLOWERS_PER_MACHINE];
struct mica_op;
extern struct mica_op *local_req_region;
extern atomic_uint_fast64_t global_w_id, committed_global_w_id;

struct thread_params {
	int id;
};

struct latency_counters{
	uint32_t* remote_reqs;
	uint32_t* local_reqs;
	uint32_t* hot_reads;
	uint32_t* hot_writes;
	long long total_measurements;
};


struct local_latency {
	int measured_local_region;
	uint8_t local_latency_start_polling;
	char* flag_to_poll;
};

extern uint8_t protocol;
extern optik_lock_t kv_lock;
extern struct latency_counters latency_count;

void *run_worker(void *arg);
void *follower(void *arg);
void *leader(void *arg);
void *print_stats(void*);
#endif
