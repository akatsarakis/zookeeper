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
  //uint64_t local_w_id = 1;

	if (ENABLE_MULTICAST == 1 && t_id == 0)
		cyan_printf("MULTICAST IS ENABLED\n");

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

  uint32_t ack_buf_push_ptr = 0, ack_buf_pull_ptr = 0;
  uint32_t w_buf_push_ptr = 0, w_buf_pull_ptr = 0;
  struct ack_message_ud_req *ack_buffer = (struct ack_message_ud_req *)(cb->dgram_buf); // leave a slot for the credits
  struct w_message_ud_req *w_buffer = (struct w_message_ud_req *)(cb->dgram_buf + LEADER_ACK_BUF_SIZE);
//  if (t_id == 0) printf("ack buffer starts at %llu, w buffer starts at %llu\n", ack_buffer, w_buffer);
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
    pre_post_recvs(&w_buf_push_ptr, cb->dgram_qp[COMMIT_W_QP_ID], cb->dgram_buf_mr->lkey, (void *)w_buffer,
                   LEADER_W_BUF_SLOTS, LDR_MAX_RECV_W_WRS, COMMIT_W_QP_ID, LDR_W_RECV_SIZE);
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
    coh_message_count[LDR_VC_NUM][MACHINE_NUM], coh_buf_i = 0,
			ack_pop_ptr = 0, ack_size = 0, inv_push_ptr = 0, inv_size = 0,
			acks_seen[MACHINE_NUM] = {0}, invs_seen[MACHINE_NUM] = {0}, upds_seen[MACHINE_NUM] = {0};
	uint32_t cmd_count = 0;
	uint32_t trace_iter = 0;
  long long credit_tx = 0, prep_br_tx = 0, commit_br_tx = 0;

  struct recv_info *w_recv_info, *ack_recv_info;
  init_recv_info(&w_recv_info, w_buf_push_ptr, LEADER_W_BUF_SLOTS,
                 (uint32_t)LDR_W_RECV_SIZE, LDR_MAX_RECV_W_WRS, w_recv_wr, cb->dgram_qp[COMMIT_W_QP_ID],
                 w_recv_sgl, (void*) w_buffer);

  init_recv_info(&ack_recv_info, ack_buf_push_ptr, LEADER_ACK_BUF_SLOTS,
                 (uint32_t)LDR_ACK_RECV_SIZE, 0, ack_recv_wr, cb->dgram_qp[PREP_ACK_QP_ID], ack_recv_sgl,
                 (void*) ack_buffer);



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

	uint16_t hottest_keys_pointers[HOTTEST_KEYS_TO_TRACK] = {0};
  struct pending_writes *p_writes;
  set_up_pending_writes(&p_writes, LEADER_PENDING_WRITES);
  void * prep_buf = (void *) p_writes->prep_fifo->prep_message;
  set_up_ldr_mrs(&prep_mr, prep_buf, &com_mr, (void *)com_fifo->commits, cb);

  // There are no explicit credits and therefore we need to represent the remote prepare buffer somehow,
  // such that we can interpret the incoming acks correctly
  struct fifo *remote_prep_buf;
  init_fifo(&remote_prep_buf, FLR_PREP_BUF_SLOTS * sizeof(uint32_t), FOLLOWER_MACHINE_NUM);
  uint32_t *fifo = (uint32_t *)remote_prep_buf[FOLLOWER_MACHINE_NUM -1].fifo;
  assert(fifo[FLR_PREP_BUF_SLOTS -1] == 0);

	/* ---------------------------------------------------------------------------
	------------------------------INITIALIZE STATIC STRUCTUREs--------------------
		---------------------------------------------------------------------------*/

	if (WRITE_RATIO > 0) {
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
  uint32_t wait_for_gid_dbg_counter = 0, wait_for_acks_dbg_counter = 0;
  uint32_t credit_debug_cnt[LDR_VC_NUM] = {0};
  uint32_t outstanding_prepares = 0;
	uint8_t stalled = 0, debug_polling = 0;
	struct timespec start, end;
	uint16_t debug_ptr = 0;
	green_printf("Leader %d  reached the loop \n", t_id);
//  if (t_id == 1) exit(0);
	/* ---------------------------------------------------------------------------
	------------------------------START LOOP--------------------------------
	---------------------------------------------------------------------------*/
	while(true) {
//

     if (ENABLE_ASSERTIONS)
       ldr_check_debug_cntrs(credit_debug_cnt, &wait_for_acks_dbg_counter,
                             &wait_for_gid_dbg_counter, p_writes, t_id);

		/* ---------------------------------------------------------------------------
		------------------------------ POLL FOR ACKS--------------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      poll_for_acks(ack_buffer, &ack_buf_pull_ptr, p_writes,
                    credits, cb->dgram_recv_cq[PREP_ACK_QP_ID], ack_recv_wc, ack_recv_info,
                    remote_prep_buf,
                    t_id, &wait_for_acks_dbg_counter, &outstanding_prepares);




/* ---------------------------------------------------------------------------
		------------------------------ PROPAGATE UPDATES--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      /* After propagating the acked messages we push their l_id to a prep_message buffer
       * to send the commits and clear the p_write buffer space. The reason behind that
       * is that we do not want to wait for the commit broadcast to happen to clear the
       * buffer space for new writes*/
      propagate_updates(p_writes, com_fifo, commit_resp, t_id, &wait_for_gid_dbg_counter);



    /* ---------------------------------------------------------------------------
		------------------------------ BROADCAST COMMITS--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      broadcast_commits(credits, cb, com_fifo,
                        &commit_br_tx, credit_debug_cnt, credit_wc,
                        com_send_sgl, com_send_wr, credit_recv_wr,
                        w_recv_info, t_id);




    /* ---------------------------------------------------------------------------
    ------------------------------PROBE THE CACHE--------------------------------------
    ---------------------------------------------------------------------------*/


		// Propagate the updates before probing the cache
		trace_iter = leader_batch_from_trace_to_cache(trace_iter, t_id, trace, ops,
                                                  p_writes, resp,
                                                  &latency_info, &start);


    /* ---------------------------------------------------------------------------
		------------------------------POLL FOR REMOTE WRITES--------------------------
		---------------------------------------------------------------------------*/
    // get local and remote writes back to back to increase the write batch



    /* ---------------------------------------------------------------------------
		------------------------------GET GLOBAL WRITE IDS--------------------------
		---------------------------------------------------------------------------*/
    // Assign a global write  id to each new write
    get_wids(p_writes, t_id);

    if (ENABLE_ASSERTIONS) check_ldr_p_states(p_writes, t_id);

		/* ---------------------------------------------------------------------------
		------------------------------BROADCASTS--------------------------------------
		---------------------------------------------------------------------------*/
		if (WRITE_RATIO > 0 )
			/* Poll for credits - Perofrom broadcasts(both invs and updates)
				 Post the appropriate number of credit receives before sending anything */
      broadcast_prepares(p_writes, credits, cb, credit_wc, credit_debug_cnt,
                         prep_send_sgl, prep_send_wr, &prep_br_tx, ack_recv_info,
                         remote_prep_buf, t_id,
                         &outstanding_prepares);

    assert(p_writes->size <= SESSIONS_PER_THREAD);
    for (uint16_t i = 0; i < LEADER_PENDING_WRITES - p_writes->size; i++) {
      uint16_t ptr = (p_writes->push_ptr + i) % LEADER_PENDING_WRITES;
      assert (p_writes->w_state[ptr] == INVALID);
    }

//		/* ---------------------------------------------------------------------------
//		------------------------------SEND CREDITS--------------------------------
//		---------------------------------------------------------------------------*/
//		if (WRITE_RATIO > 0) {
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

