#ifndef ARMONIA_UTILS_H
#define ARMONIA_UTILS_H

#include "cache.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
//<vasilis> Multicast
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <netinet/in.h>
#include <netdb.h>
// <vasilis>

#define DGRAM_BUF_SIZE 4096

extern uint64_t seed;

/* ---------------------------------------------------------------------------
------------------------------STATS --------------------------------------
---------------------------------------------------------------------------*/
struct stats {

	double remotes_per_worker[FOLLOWERS_PER_MACHINE];
	double locals_per_worker[FOLLOWERS_PER_MACHINE];
	double batch_size_per_worker[FOLLOWERS_PER_MACHINE];
	double aver_reqs_polled_per_worker[FOLLOWERS_PER_MACHINE];


	double batch_size_per_client[LEADERS_PER_MACHINE];
	double stalled_time_per_client[LEADERS_PER_MACHINE];
	double empty_reqs_per_client[LEADERS_PER_MACHINE];
	double cache_hits_per_client[LEADERS_PER_MACHINE];
	double remotes_per_client[LEADERS_PER_MACHINE];
	double locals_per_client[LEADERS_PER_MACHINE];
	double average_coalescing_per_client[LEADERS_PER_MACHINE];

	double updates_per_client[LEADERS_PER_MACHINE];
	double acks_per_client[LEADERS_PER_MACHINE];
	double invs_per_client[LEADERS_PER_MACHINE];

	double received_updates_per_client[LEADERS_PER_MACHINE];
	double received_acks_per_client[LEADERS_PER_MACHINE];
	double received_invs_per_client[LEADERS_PER_MACHINE];

	double write_ratio_per_client[LEADERS_PER_MACHINE];
};
void dump_stats_2_file(struct stats* st);
void append_throughput(double);
void window_stats(struct extended_cache_op *op, struct mica_resp *resp);
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
	int	clt_id;
	struct rdma_event_channel *channel;
	struct sockaddr_storage dst_in[MCAST_GROUPS_PER_CLIENT];
	struct sockaddr *dst_addr[MCAST_GROUPS_PER_CLIENT];
	struct sockaddr_storage src_in;
	struct sockaddr *src_addr;
	struct cm_qps cm_qp[MCAST_QPS];
	//Send-only stuff
	struct rdma_ud_param mcast_ud_param;

};

// this contains all data we need to perform our mcasts
struct mcast_essentials {
	struct ibv_cq *recv_cq;
	struct ibv_qp *recv_qp;
	struct ibv_mr *recv_mr;
	struct ibv_ah *send_ah;
	uint32_t qpn;
	uint32_t qkey;
};

int get_addr(char*, struct sockaddr*);
int alloc_nodes(void);
void setup_multicast(struct mcast_info*, int);
void resolve_addresses(struct mcast_info*);
void set_up_qp(struct cm_qps*, int);
void multicast_testing(struct mcast_essentials*, int , struct hrd_ctrl_blk*);

/* ---------------------------------------------------------------------------
------------------------------INITIALIZATION --------------------------------------
---------------------------------------------------------------------------*/

void createAHs(uint16_t clt_gid, struct hrd_ctrl_blk *cb);
void createAHs_for_worker(uint16_t, struct hrd_ctrl_blk*);
// Follower calls this function to conenct with the leader
void get_qps_from_one_machine(uint16_t g_id, struct hrd_ctrl_blk *cb);
// Leader calls this function to connect with its followers
void get_qps_from_all_other_machines(uint16_t g_id, struct hrd_ctrl_blk *cb);
// Used by all kinds of threads to publish their QPs
void publish_qps(uint32_t qp_num, uint32_t global_id, const char* qp_name, struct hrd_ctrl_blk *cb);
int* get_random_permutation(int n, int clt_gid, uint64_t *seed);
int parse_trace(char* path, struct trace_command **cmds, int clt_gid);


void set_up_the_buffer_space(uint16_t[], uint32_t[], uint32_t[]);
void trace_init(struct trace_command **cmds, int g_id);
void init_multicast(struct mcast_info**, struct mcast_essentials**, int, struct hrd_ctrl_blk*, int);
void set_up_queue_depths(int**, int**, int);
// Connect with Workers and Clients
void setup_connections_and_spawn_stats_thread(int, struct hrd_ctrl_blk *);
void set_up_wrs(struct wrkr_coalesce_mica_op** response_buffer, struct ibv_mr* resp_mr, struct hrd_ctrl_blk *cb, struct ibv_sge* recv_sgl,
                struct ibv_recv_wr* recv_wr, struct ibv_send_wr* wr, struct ibv_sge* sgl, uint16_t wrkr_lid);
void set_up_ops(struct extended_cache_op**, struct extended_cache_op**,
				struct extended_cache_op**, struct mica_resp**, struct mica_resp**,
				struct mica_resp**, struct key_home**, struct key_home**, struct key_home**);
void set_up_coh_ops(struct cache_op**, struct cache_op**, struct small_cache_op**,
					struct small_cache_op**, struct mica_resp*, struct mica_resp*,
					struct mica_op**, int);
// Post receives for the coherence traffic in the init phase
void post_coh_recvs(struct hrd_ctrl_blk*, int*, struct mcast_essentials*, int, void*);
// Set up the memory registrations required in the client if there is no Inlining
void set_up_mrs(struct ibv_mr**, struct ibv_mr**, struct extended_cache_op*, struct mica_op*,
				struct hrd_ctrl_blk*);

void set_up_credits(uint8_t[][MACHINE_NUM], struct ibv_send_wr*, struct ibv_sge*,
					struct ibv_recv_wr*, struct ibv_sge*, struct hrd_ctrl_blk*, int);
// Set up the remote Requests send and recv WRs
void set_up_remote_WRs(struct ibv_send_wr*, struct ibv_sge*, struct ibv_recv_wr*,
					   struct ibv_sge*, struct hrd_ctrl_blk* , int, struct ibv_mr*, int);
// Set up all coherence send and recv WRs// Set up all coherence WRs
void set_up_coh_WRs(struct ibv_send_wr*, struct ibv_sge*, struct ibv_recv_wr*, struct ibv_sge*,
					struct ibv_send_wr*, struct ibv_sge*, struct mica_op*, uint16_t,
					struct hrd_ctrl_blk*, struct ibv_mr*, struct mcast_essentials*, int);

/* ---------------------------------------------------------------------------
------------------------------LEADER--------------------------------------
---------------------------------------------------------------------------*/
// construct a prep_message-- max_size must be in bytes
void init_fifo(struct fifo **fifo, uint32_t max_size);
// Set up the receive info
void init_recv_info(struct recv_info **recv, uint32_t push_ptr, uint32_t buf_slots,
                    uint32_t slot_size, uint32_t, struct ibv_recv_wr *recv_wr,
                    struct ibv_qp * recv_qp, struct ibv_sge* recv_sgl, void* buf);

// Set up a struct that stores pending writes
void set_up_pending_writes(struct pending_writes **p_writes, uint32_t size);
// Set up a struct that points to completed writes
void set_up_completed_writes(struct completed_writes**, uint32_t);


// Set up all leader WRs
void set_up_ldr_WRs(struct ibv_send_wr*, struct ibv_sge*, struct ibv_recv_wr*, struct ibv_sge*,
                    struct ibv_send_wr*, struct ibv_sge*,
                    struct mica_op*, uint16_t, uint16_t, struct hrd_ctrl_blk*, struct ibv_mr*,
                    struct ibv_mr*, struct mcast_essentials*);
// Set up all Follower WRs
void set_up_follower_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                         struct ibv_recv_wr *prep_recv_wr, struct ibv_sge *prep_recv_sgl,
                         struct ibv_send_wr *w_send_wr, struct ibv_sge *w_send_sgl,
                         struct ibv_recv_wr *com_recv_wr, struct ibv_sge *com_recv_sgl,
                         uint16_t remote_thread,
                         struct hrd_ctrl_blk *cb, struct ibv_mr *w_mr,
                         struct mcast_essentials *mcast);

// Post receives for the coherence traffic in the init phase
void pre_post_recvs(struct hrd_ctrl_blk*, uint32_t* , bool, struct mcast_essentials*, void*,
                    uint32_t, uint32_t, uint16_t, uint32_t);
// set up some basic leader buffers
void set_up_ldr_ops(struct cache_op**, struct mica_resp**,
                    struct mica_resp**, struct mica_op**, struct commit_fifo**);
// Set up the memory registrations required in the leader if there is no Inlining
void set_up_ldr_mrs(struct ibv_mr**, struct mica_op*, struct ibv_mr**, void*,
                    struct hrd_ctrl_blk*);
// Set up the credits for leader and follower
void set_up_credits_and_WRs(uint16_t credits[][FOLLOWER_MACHINE_NUM], struct ibv_send_wr* credit_send_wr,
                            struct ibv_sge* credit_send_sgl, struct ibv_recv_wr* credit_recv_wr,
                            struct ibv_sge* credit_recv_sgl, struct hrd_ctrl_blk *cb, int protocol,
                            uint32_t max_credit_wrs, uint32_t max_credit_recvs);
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





#endif /* ARMONIA_UTILS_H */
