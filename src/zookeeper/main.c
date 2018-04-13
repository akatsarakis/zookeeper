#include "cache.h"
#include "util.h"
#include <getopt.h>
#include <stdbool.h>

//Global Vars
uint8_t protocol;
optik_lock_t kv_lock;
uint32_t* latency_counters;
struct latency_counters latency_count;
struct mica_op *local_req_region;
struct client_stats c_stats[LEADERS_PER_MACHINE];
struct worker_stats w_stats[FOLLOWERS_PER_MACHINE];
atomic_char local_recv_flag[FOLLOWERS_PER_MACHINE][LEADERS_PER_MACHINE][64]; //false sharing problem -- fixed with padding
struct remote_qp remote_wrkr_qp[WORKER_NUM_UD_QPS][WORKER_NUM];
struct remote_qp remote_clt_qp[CLIENT_NUM][CLIENT_UD_QPS];
atomic_char clt_needed_ah_ready, wrkr_needed_ah_ready;

#if ENABLE_WORKERS_CRCW == 1
struct mica_kv kv;
#endif



int main(int argc, char *argv[])
{
//	printf("asking %lu < %d \n", sizeof(struct ud_req) *
//								 LEADERS_PER_MACHINE * (MACHINE_NUM - 1) * WS_PER_WORKER, RR_SIZE / FOLLOWERS_PER_MACHINE);
//
//
//	assert(sizeof(struct mica_op) > HERD_PUT_REQ_SIZE);
//	assert(sizeof(struct ud_req) == UD_REQ_SIZE);
//	assert(sizeof(struct mica_op) == MICA_OP_SIZE);
//	assert(sizeof(struct mica_key) == KEY_SIZE);
//
//	cyan_printf("Size of worker req: %d, extra bytes: %d, ud req size: %d minimum worker"
//						" req size %d, actual size of req_size %d, extended cache ops size %d  \n",
//				WORKER_REQ_SIZE, EXTRA_WORKER_REQ_BYTES, UD_REQ_SIZE, MINIMUM_WORKER_REQ_SIZE, sizeof(struct wrkr_ud_req),
//				sizeof(struct extended_cache_op));
//	yellow_printf("Size of worker send req: %d, expected size %d  \n",
//				  sizeof(struct wrkr_coalesce_mica_op), WORKER_SEND_BUFF_SIZE);
//	assert(sizeof(struct extended_cache_op) <= sizeof(struct wrkr_ud_req) - GRH_SIZE);
//	if (WORKER_HYPERTHREADING) assert(FOLLOWERS_PER_MACHINE <= VIRTUAL_CORES_PER_SOCKET);

	// WORKER BUFFER SIZE
//	assert(EXTRA_WORKER_REQ_BYTES >= 0);
//	assert(WORKER_REQ_SIZE <= sizeof(struct wrkr_ud_req));
//	assert(BASE_VALUE_SIZE % pow2roundup(SHIFT_BITS) == 0);
//	if ((ENABLE_COALESCING == 1) && (DESIRED_COALESCING_FACTOR < MAX_COALESCE_PER_MACH)) assert(ENABLE_WORKER_COALESCING == 0);

	/* Cannot coalesce beyond 11 reqs, because when inlining is open it must be used, because NIC will read asynchronously otherwise */
//	if (CLIENT_ENABLE_INLINING == 1) assert((MAX_COALESCE_PER_MACH * HERD_GET_REQ_SIZE) + 1 <= MAXIMUM_INLINE_SIZE);

	assert(LEADER_MACHINE < MACHINE_NUM);
	int i, c;
	is_master = -1; is_client = -1;
	int num_threads = -1;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *)malloc(16 * sizeof(char));


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


//	printf("coalesce size %d worker inlining %d, client inlining %d \n",
//		   MAX_COALESCE_PER_MACH, WORKER_ENABLE_INLINING, CLIENT_ENABLE_INLINING);
//	yellow_printf("remote send queue depth %d, remote send ss batch %d \n", CLIENT_SEND_REM_Q_DEPTH, CLIENT_SS_BATCH);
	/* Launch multiple worker threads and multiple client threads */
	assert(machine_id < MACHINE_NUM && machine_id >=0);
	bool is_leader = machine_id == LEADER_MACHINE;
	num_threads =  is_leader ? LEADERS_PER_MACHINE : FOLLOWERS_PER_MACHINE;

	param_arr = malloc(num_threads * sizeof(struct thread_params));
	thread_arr = malloc((LEADERS_PER_MACHINE + FOLLOWERS_PER_MACHINE + 1) * sizeof(pthread_t));
	local_req_region = (struct mica_op *)malloc(FOLLOWERS_PER_MACHINE * LEADERS_PER_MACHINE * LOCAL_WINDOW * sizeof(struct mica_op));
	memset((struct client_stats*) c_stats, 0, LEADERS_PER_MACHINE * sizeof(struct client_stats));
	memset((struct worker_stats*) w_stats, 0, FOLLOWERS_PER_MACHINE * sizeof(struct worker_stats));
	int j, k;
	for (i = 0; i < FOLLOWERS_PER_MACHINE; i++)
		for (j = 0; j < LEADERS_PER_MACHINE; j++) {
			for (k = 0; k < LOCAL_REGIONS; k++)
				local_recv_flag[i][j][k] = 0;
			for (k = 0; k < LOCAL_WINDOW; k++) {
				int offset = OFFSET(i, j, k);
				local_req_region[offset].opcode = 0;
			}
		}

	clt_needed_ah_ready = 0;
	wrkr_needed_ah_ready = 0;
	cache_init(FOLLOWERS_PER_MACHINE, LEADERS_PER_MACHINE); // the first ids are taken by the workers

#if ENABLE_WORKERS_CRCW == 1
	mica_init(&kv, 0, 0, HERD_NUM_BKTS, HERD_LOG_CAP); // second 0 refers to numa node
	cache_populate_fixed_len(&kv, HERD_NUM_KEYS, HERD_VALUE_SIZE);
	optik_init(&kv_lock);
#endif

#if MEASURE_LATENCY == 1
	latency_count.hot_writes  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.hot_reads   = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.local_reqs  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.remote_reqs = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  latency_count.total_measurements = 0;
#endif
	pthread_attr_t attr;
	cpu_set_t cpus_c, cpus_w, cpus_stats;
	pthread_attr_init(&attr);
	int next_node_i = -1;
	int occupied_cores[TOTAL_CORES] = { 0 };
	for(i = 0; i < num_threads; i++) {
		param_arr[i].id = i;
		if (i < LEADERS_PER_MACHINE ) { // spawn clients
			int c_core = pin_client(i);
			yellow_printf("Creating client thread %d at core %d \n", param_arr[i].id, c_core);
			CPU_ZERO(&cpus_c);
			CPU_SET(c_core, &cpus_c);
			pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_c);
      pthread_create(&thread_arr[i], &attr, follower, &param_arr[i]);// change NULL here to &attr to get the thread affinity
			occupied_cores[c_core] = 1;
		}
		if ( i < FOLLOWERS_PER_MACHINE) { // spawn workers
			int w_core = pin_worker(i);
			green_printf("Creating worker thread %d at core %d \n", param_arr[i].id, w_core);
			CPU_ZERO(&cpus_w);
			CPU_SET(w_core, &cpus_w);

			pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_w);
			pthread_create(&thread_arr[i + LEADERS_PER_MACHINE], &attr, run_worker, &param_arr[i]);// change NULL here to &attr to get the thread affinity
			occupied_cores[w_core] = 1;
		}
	}


	if (ENABLE_SS_DEBUGGING == 1) {
		if (CREDITS_IN_MESSAGE != CREDITS_FOR_EACH_CLIENT) red_printf("CREDITS IN MESSAGE is bigger than 1: %d, that could cause a deadlock.. \n", CREDITS_IN_MESSAGE);
		if (MAX_CREDIT_WRS >= MIN_SS_BATCH) printf("MAX_CREDIT_WRS is %d, CREDIT_SS_BATCH is %d \n", MAX_CREDIT_WRS, CREDIT_SS_BATCH);
		if (SC_MAX_CREDIT_WRS >= MIN_SS_BATCH) printf("SC_MAX_CREDIT_WRS is %d, SC_CREDIT_SS_BATCH is %d \n", SC_MAX_CREDIT_WRS, SC_CREDIT_SS_BATCH);
		if (WORKER_MAX_BATCH >= MIN_SS_BATCH) printf("WORKER_MAX_BATCH is %d, WORKER_SS_BATCH is %d \n", WORKER_MAX_BATCH, WORKER_SS_BATCH);
		if (WINDOW_SIZE >= MIN_SS_BATCH) printf("WINDOW_SIZE is %d, CLIENT_SS_BATCH is %d \n", WINDOW_SIZE, CLIENT_SS_BATCH);
		if (MESSAGES_IN_BCAST_BATCH >= MIN_SS_BATCH) printf("MESSAGES_IN_BCAST_BATCH is %d, BROADCAST_SS_BATCH is %d \n", MESSAGES_IN_BCAST_BATCH, BROADCAST_SS_BATCH);
		if (BCAST_TO_CACHE_BATCH >= MIN_SS_BATCH) printf("BCAST_TO_CACHE_BATCH is %d, ACK_SS_BATCH is %d \n", BCAST_TO_CACHE_BATCH, ACK_SS_BATCH);
	}


	for(i = 0; i < LEADERS_PER_MACHINE + FOLLOWERS_PER_MACHINE + 1; i++)
		pthread_join(thread_arr[i], NULL);

	return 0;
}
