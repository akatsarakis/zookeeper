#include "cache.h"
#include "util.h"


//Global Vars
struct latency_counters latency_count;
struct thread_stats t_stats[LEADERS_PER_MACHINE];
struct remote_qp remote_follower_qp[FOLLOWER_MACHINE_NUM][FOLLOWERS_PER_MACHINE][FOLLOWER_QP_NUM];
struct remote_qp remote_leader_qp[LEADERS_PER_MACHINE][LEADER_QP_NUM];
atomic_char qps_are_set_up;
atomic_uint_fast64_t global_w_id, committed_global_w_id;




int main(int argc, char *argv[])
{

  green_printf("COMMIT: commit message %lu/%d, commit message ud req %llu/%d\n",
               sizeof(struct com_message), LDR_COM_SEND_SIZE,
               sizeof(struct com_message_ud_req), FLR_COM_RECV_SIZE);
  cyan_printf("ACK: ack message %lu/%d, ack message ud req %llu/%d\n",
               sizeof(struct ack_message), FLR_ACK_SEND_SIZE,
               sizeof(struct ack_message_ud_req), LDR_ACK_RECV_SIZE);
  yellow_printf("PREPARE: prepare %lu/%d, prep message %lu/%d, prep message ud req %llu/%d\n",
                sizeof(struct prepare), PREP_SIZE,
                sizeof(struct prep_message), LDR_PREP_SEND_SIZE,
                sizeof(struct prep_message_ud_req), FLR_PREP_RECV_SIZE);
  cyan_printf("Write: write %lu/%d, write message %lu/%d, write message ud req %llu/%d\n",
              sizeof(struct write), W_SIZE,
              sizeof(struct w_message), FLR_W_SEND_SIZE,
              sizeof(struct w_message_ud_req), LDR_W_RECV_SIZE);

  green_printf("LEADER PREPARE INLINING %d, LEADER PENDING WRITES %d \n",
							 LEADER_PREPARE_ENABLE_INLINING, LEADER_PENDING_WRITES);
  green_printf("FOLLOWER WRITE INLINING %d, FOLLOWER WRITE FIFO SIZE %d \n",
               FLR_W_ENABLE_INLINING, W_FIFO_SIZE);
  cyan_printf("PREPARE CREDITS %d, FLR PREPARE BUF SLOTS %d, FLR PREPARE BUF SIZE %d\n",
              PREPARE_CREDITS, FLR_PREP_BUF_SLOTS, FLR_PREP_BUF_SIZE);

  yellow_printf("Using Quorom %d , Quorum Machines %d \n", USE_QUORUM, LDR_QUORUM_OF_ACKS);

  if (ENABLE_MULTICAST) assert(MCAST_QP_NUM == MCAST_GROUPS_NUM);
	assert(LEADER_MACHINE < MACHINE_NUM);
	assert(LEADER_PENDING_WRITES >= SESSIONS_PER_THREAD);
	assert(sizeof(struct key) == TRUE_KEY_SIZE);
  assert(LEADERS_PER_MACHINE == FOLLOWERS_PER_MACHINE); // hopefully temporary restriction
  assert((W_CREDITS % LDR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert((COMMIT_CREDITS % FLR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert(sizeof(struct ack_message_ud_req) == LDR_ACK_RECV_SIZE);
  assert(sizeof(struct com_message_ud_req) == FLR_COM_RECV_SIZE);
  assert(sizeof(struct prep_message_ud_req) == FLR_PREP_RECV_SIZE);
  assert(sizeof(struct w_message_ud_req) == LDR_W_RECV_SIZE);
  assert(SESSIONS_PER_THREAD < M_16);
  assert(FLR_MAX_RECV_COM_WRS >= FLR_CREDITS_IN_MESSAGE);
  assert(CACHE_BATCH_SIZE > LEADER_PENDING_WRITES);


//
//  yellow_printf("WRITE: size of write recv slot %d size of w_message %lu , "
//           "value size %d, size of cache op %lu , sizeof udreq w message %lu \n",
//         LDR_W_RECV_SIZE, sizeof(struct w_message), VALUE_SIZE,
//         sizeof(struct cache_op), sizeof(struct w_message_ud_req));
  assert(sizeof(struct w_message_ud_req) == LDR_W_RECV_SIZE);
  assert(sizeof(struct w_message) == FLR_W_SEND_SIZE);


	int i, c;
	num_threads = -1;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *)malloc(16 * sizeof(char));
	global_w_id = 1; // DO not start from 0, because when checking for acks there is a non-zero test
  committed_global_w_id = 0;

	struct thread_params *param_arr;
	pthread_t *thread_arr;

	static struct option opts[] = {
			{ .name = "machine-id",			.has_arg = 1, .val = 'm' },
			{ .name = "is-roce",			.has_arg = 1, .val = 'r' },
			{ .name = "remote-ips",			.has_arg = 1, .val = 'i' },
			{ .name = "local-ip",			.has_arg = 1, .val = 'l' },
			{ 0 }
	};

	/* Parse and check arguments */
	while(1) {
		c = getopt_long(argc, argv, "M:t:b:N:n:c:u:m:p:r:i:l:x", opts, NULL);
		if(c == -1) {
			break;
		}
		switch (c) {
			case 'm':
				machine_id = atoi(optarg);
				break;
			case 'r':
				is_roce = atoi(optarg);
				break;
			case 'i':
				remote_IP = optarg;
				break;
			case 'l':
				local_IP = optarg;
				break;
			default:
				printf("Invalid argument %d\n", c);
				assert(false);
		}
	}

	/* Launch leader/follower threads */
	assert(machine_id < MACHINE_NUM && machine_id >=0);
  assert(!(is_roce == 1 && ENABLE_MULTICAST));
	is_leader = machine_id == LEADER_MACHINE;
	num_threads =  is_leader ? LEADERS_PER_MACHINE : FOLLOWERS_PER_MACHINE;

	param_arr = malloc(num_threads * sizeof(struct thread_params));
	thread_arr = malloc((LEADERS_PER_MACHINE + FOLLOWERS_PER_MACHINE + 1) * sizeof(pthread_t));
	memset((struct thread_stats*) t_stats, 0, LEADERS_PER_MACHINE * sizeof(struct thread_stats));

	qps_are_set_up = 0;

	cache_init(0, LEADERS_PER_MACHINE); // the first ids are taken by the workers

#if MEASURE_LATENCY == 1
	latency_count.hot_writes  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.hot_reads   = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  latency_count.total_measurements = 0;
	latency_count.max_read_lat = 0;
	latency_count.max_write_lat = 0;
#endif
	pthread_attr_t attr;
	cpu_set_t pinned_hw_threads;
	pthread_attr_init(&attr);
	int occupied_cores[TOTAL_CORES] = { 0 };
  char node_purpose[15];
  if (is_leader) sprintf(node_purpose, "Leader");
  else sprintf(node_purpose, "Follower");
	for(i = 0; i < num_threads; i++) {
		param_arr[i].id = i;
		int core = pin_thread(i);
		yellow_printf("Creating %s thread %d at core %d \n", node_purpose, param_arr[i].id, core);
		CPU_ZERO(&pinned_hw_threads);
		CPU_SET(core, &pinned_hw_threads);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &pinned_hw_threads);
		if (is_leader) pthread_create(&thread_arr[i], &attr, leader, &param_arr[i]);
		else pthread_create(&thread_arr[i], &attr, follower, &param_arr[i]);
		occupied_cores[core] = 1;
	}


	for(i = 0; i < num_threads; i++)
		pthread_join(thread_arr[i], NULL);

	return 0;
}
