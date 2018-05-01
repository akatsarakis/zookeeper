#include "util.h"
#include "inline_util.h"

// 1059 lines before refactoring
void *leader(void *arg)
{
	int poll_i, i, j;
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t t_id = params.id;
	uint16_t follower_qp_i = t_id % FOLLOWER_QP_NUM;
  uint16_t follower_id = t_id;

	if (ENABLE_MULTICAST == 1 && t_id == 0) {
		red_printf("MULTICAST IS NOT WORKING YET, PLEASE DISABLE IT\n");
		// TODO to fix it we must post receives seperately for acks and multicasts
		assert(false);
	}
	int protocol = LEADER;


	int *recv_q_depths, *send_q_depths;
  set_up_queue_depths_ldr_flr(&recv_q_depths, &send_q_depths, protocol);
	struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(t_id,	/* local_hid */
												0, -1, /* port_index, numa_node_id */
												0, 0,	/* #conn qps, uc */
												NULL, 0, -1,	/* prealloc conn buf, buf size, key */
												LEADER_QP_NUM, LEADER_BUF_SIZE,	/* num_dgram_qps, dgram_buf_size */
												MASTER_SHM_KEY + t_id, /* key */
												recv_q_depths, send_q_depths); /* Depth of the dgram RECV Q*/

  int ack_buf_push_ptr = 0, ack_buf_pull_ptr = -1;
  int w_buf_push_ptr = 0, w_buf_pull_ptr = -1;
  struct ack_message_ud_req *ack_buffer = (struct ack_message_ud_req *)(cb->dgram_buf);
  struct ud_req *w_buffer = (struct ud_req *)(cb->dgram_buf + LEADER_ACK_BUF_SIZE);
	/* ---------------------------------------------------------------------------
	------------------------------MULTICAST SET UP-------------------------------
	---------------------------------------------------------------------------*/

	struct mcast_info *mcast_data;
	struct mcast_essentials *mcast;
	// need to init mcast before sync, such that we can post recvs
	if (ENABLE_MULTICAST == 1) {
		init_multicast(&mcast_data, &mcast, t_id, cb, protocol);
		assert(mcast != NULL);
	}
	/* Fill the RECV queue that receives the Broadcasts, we need to do this early */
	if (WRITE_RATIO > 0) {
    // Pre post receives only for writes
    pre_post_recvs(cb, &w_buf_push_ptr, false, NULL, (void *)w_buffer,
                   LEADER_W_BUF_SLOTS, LDR_MAX_RECV_W_WRS, COMMIT_W_QP_ID);
  }

	/* -----------------------------------------------------
	--------------CONNECT WITH FOLLOWERS-----------------------
	---------------------------------------------------------*/
	setup_connections_and_spawn_stats_thread(t_id, cb);
	if (MULTICAST_TESTING == 1) multicast_testing(mcast, t_id, cb);

	/* -----------------------------------------------------
	--------------DECLARATIONS------------------------------
	---------------------------------------------------------*/
  // PREP_ACK_QP_ID 0: send Prepares -- receive ACKs
	struct ibv_send_wr prep_send_wr[LDR_MAX_PREP_WRS];
	struct ibv_sge prep_send_sgl[MAX_BCAST_BATCH], ack_recv_sgl[LDR_MAX_RECV_ACK_WRS];
	struct ibv_wc ack_recv_wc[LDR_MAX_RECV_ACK_WRS];
	struct ibv_recv_wr ack_recv_wr[LDR_MAX_RECV_ACK_WRS];

  // PREP_ACK_QP_ID 1: send Commits  -- receive Writes
  struct ibv_send_wr com_send_wr[LDR_MAX_COM_WRS];
  struct ibv_sge com_send_sgl[MAX_BCAST_BATCH], w_recv_sgl[LDR_MAX_RECV_W_WRS];
  struct ibv_wc w_recv_wc[LDR_MAX_RECV_W_WRS];
  struct ibv_recv_wr w_recv_wr[LDR_MAX_RECV_W_WRS];

  // FC_QP_ID 2: send Credits  -- receive Credits
  struct ibv_send_wr credit_send_wr[LDR_MAX_CREDIT_WRS];
  struct ibv_sge credit_sgl, credit_recv_sgl;
  struct ibv_wc credit_wc[LDR_MAX_CREDIT_RECV];
  struct ibv_recv_wr credit_recv_wr[LDR_MAX_CREDIT_RECV];

 	uint16_t credits[LDR_VC_NUM][FOLLOWER_MACHINE_NUM];
	uint16_t com_bcast_num = 0,
    coh_message_count[LDR_VC_NUM][MACHINE_NUM],
			inv_ops_i = 0, update_ops_i = 0, ack_ops_i, coh_buf_i = 0,
			ack_push_ptr = 0, ack_pop_ptr = 0, ack_size = 0, inv_push_ptr = 0, inv_size = 0,
			acks_seen[MACHINE_NUM] = {0}, invs_seen[MACHINE_NUM] = {0}, upds_seen[MACHINE_NUM] = {0};
	uint32_t cmd_count = 0, credit_debug_cnt = 0;
	uint32_t trace_iter = 0;
  long long credit_tx = 0, br_tx = 0, commit_br_tx = 0;


	//req_type measured_req_flag = NO_REQ;
	struct local_latency local_measure = {
			.measured_local_region = -1,
			.local_latency_start_polling = 0,
			.flag_to_poll = NULL,
	};

	struct latency_flags latency_info = {
			.measured_req_flag = NO_REQ,
			.last_measured_op_i = 0,
	};
	if (MEASURE_LATENCY && t_id == 0)
		latency_info.key_to_measure = malloc(sizeof(struct cache_key));


  // TODO DEPRICATE THOSE TWO
//	struct ibv_cq *coh_recv_cq = ENABLE_MULTICAST == 1 ? mcast->recv_cq : cb->dgram_recv_cq[BROADCAST_UD_QP_ID];
//	struct ibv_qp *coh_recv_qp = ENABLE_MULTICAST == 1 ? mcast->recv_qp : cb->dgram_qp[BROADCAST_UD_QP_ID];

	struct mica_op *coh_buf;
	struct cache_op *ack_bcast_ops, *ops;
  struct commit_fifo *com_fifo;
//	struct small_cache_op *inv_ops, *inv_to_send_ops;
//	struct key_home *key_homes, *next_key_homes, *third_key_homes;
	//struct mica_resp *resp, *next_resp, *third_resp;
//	struct mica_resp update_resp[BCAST_TO_CACHE_BATCH] = {0}, inv_resp[BCAST_TO_CACHE_BATCH];
	struct ibv_mr *prep_mr, *com_mr;
	//struct extended_cache_op *ops, *next_ops, *third_ops;
  struct write_op *w_ops[LEADER_PENDING_WRITES];
  struct mica_resp *resp, *commit_resp;
	set_up_ldr_ops(&ops, &resp, &commit_resp, &coh_buf, &com_fifo);
	set_up_ldr_mrs(&prep_mr, coh_buf, &com_mr, (void *)com_fifo->commits, cb);
	uint16_t hottest_keys_pointers[HOTTEST_KEYS_TO_TRACK] = {0};

  struct pending_writes *p_writes;
  set_up_pending_writes(&p_writes, LEADER_PENDING_WRITES);
  assert(p_writes->write_ops[LEADER_PENDING_WRITES - 1].opcode == CACHE_OP_BRC);

  struct completed_writes * c_writes;
  set_up_completed_writes(&c_writes, LEADER_PENDING_WRITES);

	/* ---------------------------------------------------------------------------
	------------------------------INITIALIZE STATIC STRUCTUREs--------------------
		---------------------------------------------------------------------------*/

	if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
    set_up_credits_and_WRs(credits, credit_send_wr, &credit_sgl, credit_recv_wr,
													 &credit_recv_sgl, cb, protocol, LDR_MAX_CREDIT_WRS, LDR_MAX_CREDIT_RECV);
		set_up_ldr_WRs(prep_send_wr, prep_send_sgl, ack_recv_wr, ack_recv_sgl,
                   com_send_wr, com_send_sgl,
                   coh_buf, t_id, follower_id, cb, prep_mr, com_mr, mcast);
	}
	// TRACE
	struct trace_command *trace;
	trace_init(&trace, t_id);

	/* ---------------------------------------------------------------------------
	------------------------------LATENCY AND DEBUG-----------------------------------
	---------------------------------------------------------------------------*/
	uint32_t stalled_counter = 0;
	uint8_t stalled = 0, debug_polling = 0;
	struct timespec start, end;
	uint16_t debug_ptr = 0;
	green_printf("Leader %d  reached the loop \n", t_id);
	/* ---------------------------------------------------------------------------
	------------------------------START LOOP--------------------------------
	---------------------------------------------------------------------------*/
	while(1) {
//
		if (unlikely(credit_debug_cnt > M_1)) {
			red_printf("Leader %d misses credits \n", t_id);
			red_printf("Prepare credits %d , Commit Credits %d \n", credits[PREP_VC][0],
					   credits[COMM_VC][0]);
			credit_debug_cnt = 0;
		}


		/* ---------------------------------------------------------------------------
		------------------------------ POLL FOR ACKS--------------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      poll_for_acks(ack_buffer, &ack_buf_pull_ptr, p_writes, c_writes,
                    credits, cb->dgram_recv_cq[PREP_ACK_QP_ID], ack_recv_wc);


/* ---------------------------------------------------------------------------
		------------------------------ PROPAGATE UPDATES--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      /* After propagating the acked messages we push their g_id to a fifo buffer
       * to send the commits and clear the p_write buffer space. The reason behind that
       * is that we do not want to wait for the commit broadcast to happen to clear the
       * buffer space for new writes*/
      com_bcast_num += propagate_updates(c_writes, p_writes, com_fifo);

    /* ---------------------------------------------------------------------------
		------------------------------ BROADCAST COMMITS--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      broadcast_commits(c_writes, p_writes, credits, cb, &com_bcast_num, commits,
                        &commit_br_tx, &credit_debug_cnt, credit_wc,
                        com_send_sgl, com_send_wr, credit_recv_wr);


		/* ---------------------------------------------------------------------------
		------------------------------SEND UPDS AND ACKS TO THE CACHE------------------
		---------------------------------------------------------------------------*/
		if (WRITE_RATIO > 0) {
			// Propagate updates and acks to the cache, find all acks that are complete to broadcast updates
			if (ENABLE_ASSERTIONS == 1) assert(update_ops_i <= BCAST_TO_CACHE_BATCH);
			if (update_ops_i > 0) {
//				cache_batch_op_lin_non_stalling_sessions_with_cache_op(update_ops_i, t_id, &update_ops,
//																	   update_resp);
				// the bookkeeping involves the wak-up logic and the latency measurement for the hot writes
//				updates_and_acks_bookkeeping(update_ops_i, update_ops, &latency_info, ops, &start,
//											 t_id, resp, update_resp, coh_message_count,
//											 ack_bcast_ops, &ack_push_ptr, &ack_size);
			}
		}
		/* ---------------------------------------------------------------------------
		------------------------------PROBE THE CACHE--------------------------------------
		---------------------------------------------------------------------------*/


		// Propagate the updates before probing the cache
		trace_iter = leader_batch_from_trace_to_cache(trace_iter, t_id, trace, ops,
                                                  p_writes, resp,
                                                  &latency_info, &start);
    // Assign a global write  id to each new write
    get_wids(p_writes, c_writes, t_id);


		/* ---------------------------------------------------------------------------
		------------------------------BROADCASTS--------------------------------------
		---------------------------------------------------------------------------*/
		if (WRITE_RATIO > 0 && DISABLE_CACHE == 0)
			/* Poll for credits - Perofrom broadcasts(both invs and updates)
				 Post the appropriate number of credit receives before sending anything */
			perform_broadcasts(&ack_size, p_writes, ack_bcast_ops, &ack_pop_ptr, credits,
                         cb, credit_wc, &credit_debug_cnt, prep_send_sgl, prep_send_wr, coh_message_count,
                         coh_buf, &coh_buf_i, &br_tx, &commit_br_tx, credit_recv_wr, t_id, protocol, ack_recv_sgl,
                         &ack_buf_push_ptr, ack_recv_wr, cb->dgram_qp[PREP_ACK_QP_ID], LEADER_BUF_SLOTS, (void*)ack_buffer);
//    printf("Thread %d, broadcasts are done %llu \n", t_id, br_tx);

//		/* ---------------------------------------------------------------------------
//		------------------------------SEND CREDITS--------------------------------
//		---------------------------------------------------------------------------*/
//		if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
//			/* Find out how many buffer slots have been emptied and create the appropriate
//				credit messages, for the different types of buffers (Acks, Invs, Upds)
//				If credits must be sent back, then receives for new coherence messages have to be posted first*/
//			credit_wr_i = forge_credits_LIN(coh_message_count, acks_seen, invs_seen, upds_seen, t_id,
//											credit_send_wr, &credit_tx,
//											cb, coh_recv_cq, ack_recv_wc);
//			if (credit_wr_i > 0)
//				send_credits(credit_wr_i, ack_recv_sgl, cb, &ack_buf_push_ptr, ack_recv_wr, cb->dgram_qp[BROADCAST_UD_QP_ID],
//							 credit_send_wr, (uint16_t)CREDITS_IN_MESSAGE, (uint32_t)LIN_CLT_BUF_SLOTS, (void*)ack_buffer);
//		}


//		op_i = 0; bool is_leader_t = true;
//		run_through_rest_of_ops(ops, next_ops, resp, next_resp, &op_i, &next_op_i, t_id, &latency_info, is_leader_t);

	}
	return NULL;
}

