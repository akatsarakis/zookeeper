#include "util.h"
#include "inline_util.h"

void *follower(void *arg)
{
  int poll_i, i, j;
  struct thread_params params = *(struct thread_params *) arg;
  int global_id = machine_id > LEADER_MACHINE ? ((machine_id - 1) * FOLLOWERS_PER_MACHINE) + params.id :
                  (machine_id * FOLLOWERS_PER_MACHINE) + params.id;
  uint16_t t_id = params.id;
  uint16_t leader_qp_i = (uint16_t) ((t_id + machine_id) % FOLLOWER_QP_NUM);

  if (ENABLE_MULTICAST == 1 && t_id == 0) {
      red_printf("MULTICAST IS NOT WORKING YET, PLEASE DISABLE IT\n");
      // TODO to fix it we must post receives seperately for acks and multicasts
      assert(false);
  }
  int protocol = FOLLOWER;


  int *recv_q_depths, *send_q_depths;
  set_up_queue_depths_ldr_flr(&recv_q_depths, &send_q_depths, protocol);
  struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(t_id,	/* local_hid */
                                              0, -1, /* port_index, numa_node_id */
                                              0, 0,	/* #conn qps, uc */
                                              NULL, 0, -1,	/* prealloc conn buf, buf size, key */
                                              LEADER_QP_NUM, FLR_BUF_SIZE,	/* num_dgram_qps, dgram_buf_size */
                                              MASTER_SHM_KEY + t_id, /* key */
                                              recv_q_depths, send_q_depths); /* Depth of the dgram RECV Q*/

  uint32_t prep_push_ptr = 0, prep_pull_ptr = 0;
  uint32_t com_push_ptr = 0, com_pull_ptr = 0;
  struct ud_req *prep_buffer = (struct ud_req *)(cb->dgram_buf);
  struct ud_req *com_buffer = (struct ud_req *)(cb->dgram_buf + FLR_PREP_BUF_SLOTS);
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
  /* Fill the RECV queues that receive the Commits and Prepares, (we need to do this early) */
  if (WRITE_RATIO > 0) {
    pre_post_recvs(cb, &prep_push_ptr, false, NULL, (void *) prep_buffer,
                   FLR_PREP_BUF_SLOTS, FLR_MAX_RECV_PREP_WRS, PREP_ACK_QP_ID, FLR_PREP_RECV_SIZE);
    pre_post_recvs(cb, &com_push_ptr, false, NULL, (void *) com_buffer,
                   FLR_COM_BUF_SLOTS, FLR_MAX_RECV_COM_WRS, COMMIT_W_QP_ID, FLR_COM_RECV_SIZE);
  }
  /* -----------------------------------------------------
  --------------CONNECT WITH FOLLOWERS-----------------------
  ---------------------------------------------------------*/
  setup_connections_and_spawn_stats_thread(global_id, cb);
  if (MULTICAST_TESTING == 1) multicast_testing(mcast, t_id, cb);

  /* -----------------------------------------------------
  --------------DECLARATIONS------------------------------
  ---------------------------------------------------------*/
  // PREP_ACK_QP_ID 0: send ACKS -- receive Preparess
  struct ibv_send_wr ack_send_wr[FLR_MAX_ACK_WRS];
  struct ibv_sge ack_send_sgl[FLR_MAX_ACK_WRS], prep_recv_sgl[FLR_MAX_RECV_PREP_WRS];
  struct ibv_wc prep_recv_wc[FLR_MAX_RECV_PREP_WRS];
  struct ibv_recv_wr prep_recv_wr[FLR_MAX_RECV_PREP_WRS];

  // PREP_ACK_QP_ID 1: send Writes  -- receive Commits
  struct ibv_send_wr w_send_wr[FLR_MAX_W_WRS];
  struct ibv_sge w_send_sgl[FLR_MAX_W_WRS], com_recv_sgl[FLR_MAX_RECV_COM_WRS];
  struct ibv_wc com_recv_wc[FLR_MAX_RECV_COM_WRS];
  struct ibv_recv_wr com_recv_wr[FLR_MAX_RECV_COM_WRS];

  // FC_QP_ID 2: send Credits  -- receive Credits
  struct ibv_send_wr credit_send_wr[FLR_MAX_CREDIT_WRS];
  struct ibv_sge credit_sgl, credit_recv_sgl;
  struct ibv_wc credit_wc[FLR_MAX_CREDIT_RECV];
  struct ibv_recv_wr credit_recv_wr[FLR_MAX_CREDIT_RECV];
  uint16_t credits[FLR_VC_NUM];
  uint16_t wn = 0, rm_id = 0, wr_i = 0, br_i = 0, cb_i = 0, coh_message_count[VIRTUAL_CHANNELS][MACHINE_NUM],
    credit_wr_i = 0, op_i = 0, upd_i = 0,	inv_ops_i = 0, update_ops_i = 0, ack_ops_i, coh_buf_i = 0,
    upd_count, send_ack_count, stalled_ops_i, updates_sent, credit_recv_counter = 0, rem_req_i = 0, prev_rem_req_i,
    ack_recv_counter = 0, next_op_i = 0, previous_wr_i, worker_id, remote_clt_id, min_batch_ability,
    ack_push_ptr = 0, ack_pop_ptr = 0, ack_size = 0, inv_push_ptr = 0, inv_size = 0,// last_measured_op_i = 0,
    ws[FOLLOWERS_PER_MACHINE] = {0},	/* Window slot to use for a  LOCAL worker */
    acks_seen[MACHINE_NUM] = {0}, invs_seen[MACHINE_NUM] = {0}, upds_seen[MACHINE_NUM] = {0};
  uint32_t cmd_count = 0, credit_debug_cnt = 0, outstanding_rem_reqs = 0;
  long long trace_iter = 0, credit_tx = 0, br_tx = 0, sent_ack_tx = 0;
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


  // These are useful as the follower does receive mcasts
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



  /* ---------------------------------------------------------------------------
  ------------------------------INITIALIZE STATIC STRUCTUREs--------------------
    ---------------------------------------------------------------------------*/
  // SEND AND RECEIVE WRs
//  set_up_remote_WRs(w_send_wr, w_send_sgl, prep_recv_wr, &prep_recv_sgl, cb, t_id, ops_mr, protocol);
  if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
//      set_up_credits(credits, credit_send_wr, &credit_sgl, credit_recv_wr, &credit_recv_sgl, cb, protocol);
//      set_up_coh_WRs(ack_send_wr, ack_send_sgl, com_recv_wr, com_recv_sgl,
//                     ack_wr, ack_sgl, coh_buf, t_id, cb, coh_mr, mcast, protocol);
  }
  // TRACE
  struct trace_command *trace;
  trace_init(&trace, t_id);
  /* ---------------------------------------------------------------------------
  ------------------------------Prepost RECVS-----------------------------------
  ---------------------------------------------------------------------------*/
  /* Remote Requests */
//    for(i = 0; i < WINDOW_SIZE; i++) {
//        hrd_post_dgram_recv(cb->dgram_qp[REMOTE_UD_QP_ID],
//                            (void *) cb->dgram_buf, remote_buf_size, cb->dgram_buf_mr->lkey);
//    }
  /* ---------------------------------------------------------------------------
  ------------------------------LATENCY AND DEBUG-----------------------------------
  ---------------------------------------------------------------------------*/
  uint32_t stalled_counter = 0;
  uint8_t stalled = 0, debug_polling = 0;
  struct timespec start, end;
  uint16_t debug_ptr = 0;
  green_printf("Follower %d  reached the loop \n", t_id);
  /* ---------------------------------------------------------------------------
  ------------------------------START LOOP--------------------------------
  ---------------------------------------------------------------------------*/
  while(1) {
      // Swap the op buffers to facilitate correct ordering
      swap_ops(&ops, &next_ops, &third_ops,
               &resp, &next_resp, &third_resp,
              NULL, NULL, NULL);

      if (unlikely(credit_debug_cnt > M_1)) {
          red_printf("Follower %d misses credits \n", t_id);
//          red_printf("Ack credits %d , inv Credits %d , UPD credits %d \n", credits[ACK_VC][(machine_id + 1) % MACHINE_NUM],
//                     credits[INV_VC][(machine_id + 1) % MACHINE_NUM], credits[UPD_VC][(machine_id + 1) % MACHINE_NUM]);
          credit_debug_cnt = 0;
      }


  /* ---------------------------------------------------------------------------
  ------------------------------ POLL BROADCAST REGION--------------------------
  ---------------------------------------------------------------------------*/
//      memset(coh_message_count, 0, VIRTUAL_CHANNELS * MACHINE_NUM * sizeof(uint16_t));
//      upd_count = 0;
//      update_ops_i = 0;
//      inv_ops_i = 0;
//      ack_ops_i = 0;
      uint16_t polled_messages = 0;
      if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
          poll_coherence_LIN(&update_ops_i, prep_buffer, &prep_pull_ptr, update_ops, t_id, t_id,
                             ack_size, &polled_messages, ack_ops_i, inv_size, &inv_ops_i, inv_ops);
      }
  /* ---------------------------------------------------------------------------
  ------------------------------SEND UPDS AND ACKS TO THE CACHE------------------
  ---------------------------------------------------------------------------*/
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
      trace_iter = batch_from_trace_to_cache(trace_iter, t_id, trace, ops,
                                             resp, key_homes, 0, next_op_i,
                                             &latency_info, &start, hottest_keys_pointers);

  // Print out some window stats periodically
  if (ENABLE_WINDOW_STATS == 1 && trigger_measurement(t_id))
    window_stats(ops, resp);


  /* ---------------------------------------------------------------------------
  ------------------------------SEND INVS TO THE CACHE---------------------------
  ---------------------------------------------------------------------------*/
  // As the beetles did not say "all we are saying, is give reads a chance"
  if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
    if (inv_ops_i > 0) {
      // Proapagate the invalidations to the cache
      cache_batch_op_lin_non_stalling_sessions_with_small_cache_op(inv_ops_i, t_id, &inv_ops,
                                     inv_resp);
      /* Create an array with acks to send out such that we send answers only
       to the invalidations that succeeded, take care to keep the back pressure:
       invs that failed trigger credits to be sent back, successful invs still
       hold buffer space though */

      invs_bookkeeping(inv_ops_i, inv_resp, coh_message_count, inv_ops,
               inv_to_send_ops, &inv_push_ptr, &inv_size, &debug_ptr);
    }
  }

  /* ---------------------------------------------------------------------------
  ------------------------------ACKNOWLEDGEMENTS--------------------------------
  ---------------------------------------------------------------------------*/

  if (WRITE_RATIO > 0 && DISABLE_CACHE == 0)
    if (inv_size > 0)
//      send_acks(inv_to_send_ops, credits, credit_wc, ack_wr, ack_sgl, &sent_ack_tx, &inv_size,
//            &ack_recv_counter, credit_recv_wr, t_id, coh_message_count, cb, debug_ptr);

  /* ---------------------------------------------------------------------------
  ------------------------------SEND CREDITS--------------------------------
  ---------------------------------------------------------------------------*/
  if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) {
    /* Find out how many buffer slots have been emptied and create the appropriate
      credit messages, for the different types of buffers (Acks, Invs, Upds)
      If credits must be sent back, then receives for new coherence messages have to be posted first*/
//    credit_wr_i = forge_credits_LIN(coh_message_count, acks_seen, invs_seen, upds_seen, t_id,
//                    credit_wr, &credit_tx,
//                    cb, coh_recv_cq, coh_wc);
//    if (credit_wr_i > 0)
//      send_credits(credit_wr_i, coh_recv_sgl, cb, &prep_push_ptr, coh_recv_wr, cb->dgram_qp[BROADCAST_UD_QP_ID],
//             credit_wr, (uint16_t)CREDITS_IN_MESSAGE, (uint32_t)LIN_CLT_BUF_SLOTS, (void*)prep_buffer);
  }
      op_i = 0; bool is_leader_t = false;
      run_through_rest_of_ops(ops, next_ops, resp, next_resp, &op_i, &next_op_i, t_id, &latency_info, is_leader_t);
  }
  return NULL;
}
