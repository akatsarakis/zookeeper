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

	uint16_t remote_buf_size =  ENABLE_WORKER_COALESCING == 1 ?
								(GRH_SIZE + sizeof(struct wrkr_coalesce_mica_op)) : UD_REQ_SIZE ;
	int *recv_q_depths, *send_q_depths;
  set_up_queue_depths_ldr_flr(&recv_q_depths, &send_q_depths, protocol);
	struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(t_id,	/* local_hid */
												0, -1, /* port_index, numa_node_id */
												0, 0,	/* #conn qps, uc */
												NULL, 0, -1,	/* prealloc conn buf, buf size, key */
												LEADER_QP_NUM, remote_buf_size + LIN_CLT_BUF_SIZE,	/* num_dgram_qps, dgram_buf_size */
												MASTER_SHM_KEY + FOLLOWERS_PER_MACHINE + t_id, /* key */
												recv_q_depths, send_q_depths); /* Depth of the dgram RECV Q*/

	int push_ptr = 0, pull_ptr = -1;
	struct ud_req *incoming_reqs = (struct ud_req *)(cb->dgram_buf + remote_buf_size);
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
	if (WRITE_RATIO > 0 && DISABLE_CACHE == 0)
		pre_post_recvs(cb, &push_ptr, mcast, (void*)incoming_reqs, LEADER_BUF_SLOTS, LDR_PREPOST_RECEIVES_NUM);

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
	struct ibv_sge coh_send_sgl[MAX_BCAST_BATCH], ack_recv_sgl[LDR_MAX_RECV_ACK_WRS];
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

 	uint8_t credits[VIRTUAL_CHANNELS][FOLLOWER_MACHINE_NUM];
	uint16_t coh_message_count[VIRTUAL_CHANNELS][MACHINE_NUM],
			inv_ops_i = 0, update_ops_i = 0, ack_ops_i, coh_buf_i = 0,
			ack_push_ptr = 0, ack_pop_ptr = 0, ack_size = 0, inv_push_ptr = 0, inv_size = 0,
			acks_seen[MACHINE_NUM] = {0}, invs_seen[MACHINE_NUM] = {0}, upds_seen[MACHINE_NUM] = {0};
	uint32_t cmd_count = 0, credit_debug_cnt = 0;
	uint32_t trace_iter = 0, credit_tx = 0, br_tx = 0, commit_br_tx = 0;


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



	struct ibv_cq *coh_recv_cq = ENABLE_MULTICAST == 1 ? mcast->recv_cq : cb->dgram_recv_cq[BROADCAST_UD_QP_ID];
	struct ibv_qp *coh_recv_qp = ENABLE_MULTICAST == 1 ? mcast->recv_qp : cb->dgram_qp[BROADCAST_UD_QP_ID];

	struct mica_op *coh_buf;
	struct cache_op *update_ops, *ack_bcast_ops;
	struct small_cache_op *inv_ops, *inv_to_send_ops;
	struct key_home *key_homes, *next_key_homes, *third_key_homes;
	struct mica_resp *resp, *next_resp, *third_resp;
	struct mica_resp update_resp[BCAST_TO_CACHE_BATCH] = {0}, inv_resp[BCAST_TO_CACHE_BATCH];
	struct ibv_mr *ops_mr, *coh_mr;
	struct extended_cache_op *ops, *next_ops, *third_ops;
	set_up_ops(&ops, &next_ops, &third_ops, &resp, &next_resp, &third_resp,
			   &key_homes, &next_key_homes, &third_key_homes);
	set_up_coh_ops(&update_ops, &ack_bcast_ops, &inv_ops, &inv_to_send_ops, update_resp, inv_resp, &coh_buf, protocol);
	set_up_mrs(&ops_mr, &coh_mr, ops, coh_buf, cb);
	uint16_t hottest_keys_pointers[HOTTEST_KEYS_TO_TRACK] = {0};

	  struct pending_writes *p_writes;
  set_up_pending_writes(&p_writes, LEADER_PENDING_WRITES);
  assert(p_writes->write_ops[LEADER_PENDING_WRITES - 1].opcode == CACHE_OP_BRC);

	/* ---------------------------------------------------------------------------
	------------------------------INITIALIZE STATIC STRUCTUREs--------------------
		---------------------------------------------------------------------------*/

	if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
		set_up_credits(credits, credit_send_wr, &credit_sgl, credit_recv_wr, &credit_recv_sgl, cb, protocol);
		set_up_ldr_WRs(prep_send_wr, coh_send_sgl, ack_recv_wr, ack_recv_sgl,
                   coh_buf, t_id, follower_id, cb, coh_mr, mcast, protocol);
	}
	// TRACE
	struct trace_command *trace;
	trace_init(&trace, t_id);
	/* ---------------------------------------------------------------------------
	------------------------------Prepost RECVS-----------------------------------
	---------------------------------------------------------------------------*/
	/* Remote Requests */
	for(i = 0; i < WINDOW_SIZE; i++) {
		hrd_post_dgram_recv(cb->dgram_qp[REMOTE_UD_QP_ID],
							(void *) cb->dgram_buf, remote_buf_size, cb->dgram_buf_mr->lkey);
	}
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
		// Swap the op buffers to facilitate correct ordering
		swap_ops(&ops, &next_ops, &third_ops,
				 &resp, &next_resp, &third_resp,
				 &key_homes, &next_key_homes, &third_key_homes);
//
		if (unlikely(credit_debug_cnt > M_1)) {
			red_printf("Client %d misses credits \n", t_id);
			red_printf("Ack credits %d , inv Credits %d , UPD credits %d \n", credits[ACK_VC][(machine_id + 1) % MACHINE_NUM],
					   credits[INV_VC][(machine_id + 1) % MACHINE_NUM], credits[UPD_VC][(machine_id + 1) % MACHINE_NUM]);
			credit_debug_cnt = 0;
		}
//		/* ---------------------------------------------------------------------------
//		------------------------------Refill REMOTE RECVS--------------------------------
//		---------------------------------------------------------------------------*/
//
//		refill_recvs(rem_req_i, rem_recv_wr, cb);
//
//		/* ---------------------------------------------------------------------------
//		------------------------------ POLL BROADCAST REGION--------------------------
//		---------------------------------------------------------------------------*/
		memset(coh_message_count, 0, VIRTUAL_CHANNELS * MACHINE_NUM * sizeof(uint16_t));
		upd_count = 0;
		update_ops_i = 0;
		inv_ops_i = 0;
		ack_ops_i = 0;
		uint16_t polled_messages = 0;
		if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
			poll_coherence_LIN(&update_ops_i, incoming_reqs, &pull_ptr, update_ops, t_id, t_id,
							   ack_size, &polled_messages, ack_ops_i, inv_size, &inv_ops_i, inv_ops);
		}
//    for (int i = 0; i < LEADER_PENDING_WRITES; ++i) {
//      if (p_writes->w_state[i] == SENT) printf("Sent pending write %d \n", i);
//    }
//		/* ---------------------------------------------------------------------------
//		------------------------------SEND UPDS AND ACKS TO THE CACHE------------------
//		---------------------------------------------------------------------------*/
		if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
			// Propagate updates and acks to the cache, find all acks that are complete to broadcast updates
			if (ENABLE_ASSERTIONS == 1) assert(update_ops_i <= BCAST_TO_CACHE_BATCH);
			if (update_ops_i > 0) {
				cache_batch_op_lin_non_stalling_sessions_with_cache_op(update_ops_i, t_id, &update_ops,
																	   update_resp);
				// the bookkeeping involves the wak-up logic and the latency measurement for the hot writes
				updates_and_acks_bookkeeping(update_ops_i, update_ops, &latency_info, ops, &start,
											 t_id, resp, update_resp, coh_message_count,
											 ack_bcast_ops, &ack_push_ptr, &ack_size);
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
    get_wids(p_writes, t_id);


		/* ---------------------------------------------------------------------------
		------------------------------BROADCASTS--------------------------------------
		---------------------------------------------------------------------------*/
		if (WRITE_RATIO > 0 && DISABLE_CACHE == 0)
			/* Poll for credits - Perofrom broadcasts(both invs and updates)
				 Post the appropriate number of credit receives before sending anything */
			perform_broadcasts(&ack_size, p_writes, ack_bcast_ops, &ack_pop_ptr, credits,
                         cb, credit_wc, &credit_debug_cnt, coh_send_sgl, prep_send_wr, coh_message_count,
                         coh_buf, &coh_buf_i, &br_tx, &commit_br_tx, credit_recv_wr, t_id, protocol, ack_recv_sgl,
                         &push_ptr, ack_recv_wr, coh_recv_qp, LEADER_BUF_SLOTS, (void*)incoming_reqs);
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
//				send_credits(credit_wr_i, ack_recv_sgl, cb, &push_ptr, ack_recv_wr, cb->dgram_qp[BROADCAST_UD_QP_ID],
//							 credit_send_wr, (uint16_t)CREDITS_IN_MESSAGE, (uint32_t)LIN_CLT_BUF_SLOTS, (void*)incoming_reqs);
//		}


//		op_i = 0; bool is_leader_t = true;
//		run_through_rest_of_ops(ops, next_ops, resp, next_resp, &op_i, &next_op_i, t_id, &latency_info, is_leader_t);

	}
	return NULL;
}

