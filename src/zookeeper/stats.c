#include "util.h"
#include "inline_util.h"

void print_latency_stats(void);

void *print_stats(void* no_arg) {
  int j;
  uint16_t i, print_count = 0;
  long long all_clients_cache_hits = 0;
  double total_throughput = 0;

  int sleep_time = 20;
  struct thread_stats *curr_c_stats, *prev_c_stats;
  curr_c_stats = (struct thread_stats *) malloc(num_threads * sizeof(struct thread_stats));
  prev_c_stats = (struct thread_stats *) malloc(num_threads * sizeof(struct thread_stats));
  struct stats all_stats;
  sleep(4);
  memcpy(prev_c_stats, (void *) t_stats, num_threads * (sizeof(struct thread_stats)));
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);
  while (true) {
    sleep(sleep_time);
    clock_gettime(CLOCK_REALTIME, &end);
    double seconds = (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1000000001;
    start = end;
    memcpy(curr_c_stats, (void *) t_stats, num_threads * (sizeof(struct thread_stats)));
//        memcpy(curr_w_stats, (void*) f_stats, FOLLOWERS_PER_MACHINE * (sizeof(struct follower_stats)));
    all_clients_cache_hits = 0;
    print_count++;
    if (EXIT_ON_PRINT == 1 && print_count == PRINT_NUM) {
      if (MEASURE_LATENCY && machine_id == 0) print_latency_stats();
      printf("---------------------------------------\n");
      printf("------------RUN TERMINATED-------------\n");
      printf("---------------------------------------\n");
      exit(0);
    }
    seconds *= MILLION; // compute only MIOPS
    for (i = 0; i < num_threads; i++) {
      all_clients_cache_hits += curr_c_stats[i].cache_hits_per_thread - prev_c_stats[i].cache_hits_per_thread;
      all_stats.cache_hits_per_thread[i] =
        (curr_c_stats[i].cache_hits_per_thread - prev_c_stats[i].cache_hits_per_thread) / seconds;

      all_stats.stalled_gid[i] = (curr_c_stats[i].stalled_gid - prev_c_stats[i].stalled_gid) / seconds;
      all_stats.stalled_ack_prep[i] = (curr_c_stats[i].stalled_ack_prep - prev_c_stats[i].stalled_ack_prep) / seconds;
      all_stats.stalled_com_credit[i] =
        (curr_c_stats[i].stalled_com_credit - prev_c_stats[i].stalled_com_credit) / seconds;

      all_stats.preps_sent[i] = (curr_c_stats[i].preps_sent - prev_c_stats[i].preps_sent) / seconds;
      all_stats.coms_sent[i] = (curr_c_stats[i].coms_sent - prev_c_stats[i].coms_sent) / seconds;
      all_stats.acks_sent[i] = (curr_c_stats[i].acks_sent - prev_c_stats[i].acks_sent) / seconds;
      all_stats.received_coms[i] = (curr_c_stats[i].received_coms - prev_c_stats[i].received_coms) / seconds;
      all_stats.received_preps[i] = (curr_c_stats[i].received_preps - prev_c_stats[i].received_preps) / seconds;
      all_stats.received_acks[i] = (curr_c_stats[i].received_acks - prev_c_stats[i].received_acks) / seconds;
    }

      memcpy(prev_c_stats, curr_c_stats, num_threads * (sizeof(struct thread_stats)));
      total_throughput = (all_clients_cache_hits) / seconds;

      printf("---------------PRINT %d time elapsed %.2f---------------\n", print_count, seconds / MILLION);
      green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
      for (i = 0; i < num_threads; i++) {
        yellow_printf("T%d: %.2f MIOPS, STALL: GID: %.2f/s, ACK/PREP %.2f/s, COM/CREDIT %.2f/s ", i,
                      all_stats.cache_hits_per_thread[i],
                      all_stats.stalled_gid[i],
                      all_stats.stalled_ack_prep[i],
                      all_stats.stalled_com_credit[i]);
        if (i > 0 && i % 2 == 0) printf("\n");
      }
      printf("\n");
      printf("---------------------------------------\n");
      if (ENABLE_CACHE_STATS == 1)
        print_cache_stats(start, machine_id);
      // // Write to a file all_clients_throughput, per_worker_remote_throughput[], per_worker_local_throughput[]
      if (DUMP_STATS_2_FILE == 1)
        dump_stats_2_file(&all_stats);
      green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
  }

}

//assuming microsecond latency
void print_latency_stats(void){
    uint8_t protocol = LEADER;
    FILE *latency_stats_fd;
    int i = 0;
    char filename[128];
    char* path = "../../results/latency";
    const char * exectype[] = {
            "BS", //baseline
            "SC", //Sequential Consistency
            "LIN", //Linearizability (non stalling)
            "SS" //Strong Consistency (stalling)
    };

    sprintf(filename, "%s/latency_stats_%s_%s_%s_s_%d_a_%d_v_%d_m_%d_c_%d_w_%d_r_%d%s_C_%d.csv", path,
            DISABLE_CACHE == 1 ? "BS" : exectype[protocol],
            LOAD_BALANCE == 1 ? "UNIF" : "SKEW",
            EMULATING_CREW == 1 ? "CREW" : "EREW",
            DISABLE_CACHE == 0 && protocol == 2 && ENABLE_MULTIPLE_SESSIONS != 0 && SESSIONS_PER_THREAD != 0 ? SESSIONS_PER_THREAD: 0,
            SKEW_EXPONENT_A,
            USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
            MACHINE_NUM, num_threads,
            FOLLOWERS_PER_MACHINE, WRITE_RATIO,
            BALANCE_HOT_WRITES == 1  ? "_lbw" : "",
            CACHE_BATCH_SIZE);

    latency_stats_fd = fopen(filename, "w");
    fprintf(latency_stats_fd, "#---------------- Remote Reqs --------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "rr: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.remote_reqs[i]);
    fprintf(latency_stats_fd, "rr: -1, %d\n",latency_count.remote_reqs[LATENCY_BUCKETS]); //print outliers

    fprintf(latency_stats_fd, "#---------------- Local Reqs ---------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "lr: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.local_reqs[i]);
    fprintf(latency_stats_fd, "lr: -1, %d\n",latency_count.local_reqs[LATENCY_BUCKETS]); //print outliers

    fprintf(latency_stats_fd, "#---------------- Hot Reads ----------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "hr: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.hot_reads[i]);
    fprintf(latency_stats_fd, "hr: -1, %d\n",latency_count.hot_reads[LATENCY_BUCKETS]); //print outliers

    fprintf(latency_stats_fd, "#---------------- Hot Writes ---------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "hw: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.hot_writes[i]);
    fprintf(latency_stats_fd, "hw: -1, %d\n",latency_count.hot_writes[LATENCY_BUCKETS]); //print outliers

    fclose(latency_stats_fd);

    printf("Latency stats saved at %s\n", filename);
}
