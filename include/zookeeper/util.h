#ifndef ARMONIA_UTILS_H
#define ARMONIA_UTILS_H

#include "cache.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
//Multicast
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <netinet/in.h>
#include <netdb.h>
//



extern uint64_t seed;

/* ---------------------------------------------------------------------------
------------------------------STATS --------------------------------------
---------------------------------------------------------------------------*/
struct stats {
  double batch_size_per_thread[THREADS_PER_MACHINE];
  double com_batch_size[THREADS_PER_MACHINE];
  double prep_batch_size[THREADS_PER_MACHINE];
  double ack_batch_size[THREADS_PER_MACHINE];
  double write_batch_size[THREADS_PER_MACHINE];
	double stalled_gid[THREADS_PER_MACHINE];
  double stalled_ack_prep[THREADS_PER_MACHINE];
  double stalled_com_credit[THREADS_PER_MACHINE];


	double cache_hits_per_thread[THREADS_PER_MACHINE];


	double preps_sent[THREADS_PER_MACHINE];
	double acks_sent[THREADS_PER_MACHINE];
	double coms_sent[THREADS_PER_MACHINE];

	double received_coms[THREADS_PER_MACHINE];
	double received_acks[THREADS_PER_MACHINE];
	double received_preps[THREADS_PER_MACHINE];

	double write_ratio_per_client[THREADS_PER_MACHINE];
};
void dump_stats_2_file(struct stats* st);
int spawn_stats_thread();
void print_latency_stats(void);


/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/
// This helps us set up the necessary rdma_cm_ids for the multicast groups
struct cm_qps
{
	int receive_q_depth;
	struct rdma_cm_id* cma_id;
	struct ibv_pd* pd;
	struct ibv_cq* cq;
	struct ibv_mr* mr;
	void *mem;
};

// This helps us set up the multicasts
struct mcast_info
{
	int	t_id;
	struct rdma_event_channel *channel;
	struct sockaddr_storage dst_in[MCAST_GROUPS_NUM];
	struct sockaddr *dst_addr[MCAST_GROUPS_NUM];
	struct sockaddr_storage src_in;
	struct sockaddr *src_addr;
	struct cm_qps cm_qp[MCAST_QPS];
	//Send-only stuff
	struct rdma_ud_param mcast_ud_param[MCAST_GROUPS_NUM];

};

// this contains all data we need to perform our mcasts
struct mcast_essentials {
	struct ibv_cq *recv_cq[MCAST_QP_NUM];
	struct ibv_qp *recv_qp[MCAST_QP_NUM];
	struct ibv_mr *recv_mr;
	struct ibv_ah *send_ah[MCAST_QP_NUM];
	uint32_t qpn[MCAST_QP_NUM];
	uint32_t qkey[MCAST_QP_NUM];
};

int get_addr(char*, struct sockaddr*);
void setup_multicast(struct mcast_info*, int*);
void resolve_addresses(struct mcast_info*);
void set_up_qp(struct cm_qps*, int*);
void multicast_testing(struct mcast_essentials*, int , struct hrd_ctrl_blk*);

/* ---------------------------------------------------------------------------
------------------------------INITIALIZATION --------------------------------------
---------------------------------------------------------------------------*/


// Follower calls this function to conenct with the leader
void get_qps_from_one_machine(uint16_t g_id, struct hrd_ctrl_blk *cb);
// Leader calls this function to connect with its followers
void get_qps_from_all_other_machines(uint16_t g_id, struct hrd_ctrl_blk *cb);
// Used by all kinds of threads to publish their QPs
void publish_qps(uint32_t qp_num, uint32_t global_id, const char* qp_name, struct hrd_ctrl_blk *cb);

int parse_trace(char* path, struct trace_command **cmds, int g_id);


void trace_init(struct trace_command **cmds, int g_id);
void init_multicast(struct mcast_info**, struct mcast_essentials**, int, struct hrd_ctrl_blk*, int);
// Connect with Workers and Clients
void setup_connections_and_spawn_stats_thread(int, struct hrd_ctrl_blk *);


/* ---------------------------------------------------------------------------
------------------------------LEADER--------------------------------------
---------------------------------------------------------------------------*/
// construct a prep_message-- max_size must be in bytes
void init_fifo(struct fifo **fifo, uint32_t max_size, uint32_t);
// Set up the receive info
void init_recv_info(struct recv_info **recv, uint32_t push_ptr, uint32_t buf_slots,
                    uint32_t slot_size, uint32_t, struct ibv_recv_wr *recv_wr,
                    struct ibv_qp * recv_qp, struct ibv_sge* recv_sgl, void* buf);

// Set up a struct that stores pending writes
void set_up_pending_writes(struct pending_writes **p_writes, uint32_t size, int);

// Set up all leader WRs
void set_up_ldr_WRs(struct ibv_send_wr*, struct ibv_sge*, struct ibv_recv_wr*, struct ibv_sge*,
                    struct ibv_send_wr*, struct ibv_sge*, struct ibv_recv_wr*, struct ibv_sge*,
                    uint16_t, uint16_t, struct hrd_ctrl_blk*, struct ibv_mr*,
                    struct ibv_mr*, struct mcast_essentials*);
// Set up all Follower WRs
void set_up_follower_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                         struct ibv_recv_wr *prep_recv_wr, struct ibv_sge *prep_recv_sgl,
                         struct ibv_send_wr *w_send_wr, struct ibv_sge *w_send_sgl,
                         struct ibv_recv_wr *com_recv_wr, struct ibv_sge *com_recv_sgl,
                         uint16_t remote_thread,
                         struct hrd_ctrl_blk *cb, struct ibv_mr *w_mr,
                         struct mcast_essentials *mcast);

// Follower sends credits for commits
void flr_set_up_credit_WRs(struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                           struct hrd_ctrl_blk *cb, uint8_t flr_id, uint32_t max_credt_wrs, uint16_t);

// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t*, struct ibv_qp *, uint32_t lkey, void*,
                    uint32_t, uint32_t, uint16_t, uint32_t);
// set up some basic leader buffers
void set_up_ldr_ops(struct cache_op**, struct mica_resp**,
                    struct commit_fifo**, uint16_t);
// Set up the memory registrations required in the leader if there is no Inlining
void set_up_ldr_mrs(struct ibv_mr**, void*, struct ibv_mr**, void*,
                    struct hrd_ctrl_blk*);
// Set up the credits for leader
void ldr_set_up_credits_and_WRs(uint16_t credits[][FOLLOWER_MACHINE_NUM], struct ibv_recv_wr *credit_recv_wr,
                                struct ibv_sge *credit_recv_sgl, struct hrd_ctrl_blk *cb,
                                uint32_t max_credit_recvs);
// Manufactures a trace without a file
void manufacture_trace(struct trace_command **cmds, int g_id);

//Set up the depths of all QPs
void set_up_queue_depths_ldr_flr(int**, int**, int);

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
// check if the given protocol is invalid
void check_protocol(int);
// pin threads starting from core 0
int pin_thread(int t_id);
// pin a thread avoid collisions with pin_thread()
int pin_threads_avoiding_collisions(int c_id);

void print_latency_stats(void);



#endif /* ARMONIA_UTILS_H */
