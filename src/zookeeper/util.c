//#include <infiniband/verbs.h>
#include "util.h"
#include "city.h"
//#ifndef _GNU_SOURCE
//# define _GNU_SOURCE
//#endif

//#include <pthread.h>

// Leader calls this function to connect with its followers
void get_qps_from_all_other_machines(uint16_t g_id, struct hrd_ctrl_blk *cb)
{
    int i, qp_i;
    int ib_port_index = 0;
    struct ibv_ah *follower_ah[FOLLOWER_NUM][FOLLOWER_QP_NUM];
    struct hrd_qp_attr *follower_qp[FOLLOWER_NUM][FOLLOWER_QP_NUM];

    // -- CONNECT WITH FOLLOWERS
    for(i = 0; i < FOLLOWER_NUM; i++) {
        for (qp_i = 0; qp_i < FOLLOWER_QP_NUM; qp_i++) {
            /* Compute the control block and physical port index for client @i */
            // int cb_i = i % num_server_ports;
            int local_port_i = ib_port_index;// + cb_i;

            char follower_name[QP_NAME_SIZE];
            sprintf(follower_name, "follower-dgram-%d-%d", i, qp_i);

            /* Get the UD queue pair for the ith machine */
            follower_qp[i][qp_i] = NULL;
//            printf("Leader %d is Looking for follower %s \n", l_id, follower_name);
            while(follower_qp[i][qp_i] == NULL) {
                follower_qp[i][qp_i] = hrd_get_published_qp(follower_name);
                if(follower_qp[i][qp_i] == NULL)
                    usleep(200000);
            }
            // printf("main:Leader %d found clt %d. Client LID: %d\n",
            //        l_id, i, follower_qp[i][qp_i]->lid);
            struct ibv_ah_attr ah_attr = {
                //-----INFINIBAND----------
                .is_global = 0,
                .dlid = (uint16_t) follower_qp[i][qp_i]->lid,
                .sl = (uint8_t) follower_qp[i][qp_i]->sl,
                .src_path_bits = 0,
                /* port_num (> 1): device-local port for responses to this worker */
                .port_num = (uint8_t) (local_port_i + 1),
            };

            // ---ROCE----------
            if (is_roce == 1) {
                ah_attr.is_global = 1;
                ah_attr.dlid = 0;
                ah_attr.grh.dgid.global.interface_id =  follower_qp[i][qp_i]->gid_global_interface_id;
                ah_attr.grh.dgid.global.subnet_prefix = follower_qp[i][qp_i]->gid_global_subnet_prefix;
                ah_attr.grh.sgid_index = 0;
                ah_attr.grh.hop_limit = 1;
            }
            //
            follower_ah[i][qp_i]= ibv_create_ah(cb->pd, &ah_attr);
            assert(follower_ah[i][qp_i] != NULL);
            remote_follower_qp[i / FOLLOWERS_PER_MACHINE][i % FOLLOWERS_PER_MACHINE][qp_i].ah =
                follower_ah[i][qp_i];
            remote_follower_qp[i / FOLLOWERS_PER_MACHINE][i % FOLLOWERS_PER_MACHINE][qp_i].qpn =
                follower_qp[i][qp_i]->qpn;
        }
    }
}

// Follower calls this function to connect with the leader
void get_qps_from_one_machine(uint16_t g_id, struct hrd_ctrl_blk *cb) {
    int i, qp_i;
    struct ibv_ah *leader_ah[LEADERS_PER_MACHINE][LEADER_QP_NUM];
    struct hrd_qp_attr *leader_qp[LEADERS_PER_MACHINE][LEADER_QP_NUM];

    for(i = 0; i < LEADERS_PER_MACHINE; i++) {
        for (qp_i = 0; qp_i < LEADER_QP_NUM; qp_i++) {
            /* Compute the control block and physical port index for leader thread @i */
            int local_port_i = 0;

            char ldr_name[QP_NAME_SIZE];
            sprintf(ldr_name, "leader-dgram-%d-%d", i, qp_i);
            /* Get the UD queue pair for the ith leader thread */
            leader_qp[i][qp_i] = NULL;
//          printf("Follower %d is Looking for leader %s\n", l_id, ldr_name);
          while (leader_qp[i][qp_i] == NULL) {
                leader_qp[i][qp_i] = hrd_get_published_qp(ldr_name);
                //printf("Follower %d is expecting leader %s\n" , l_id, ldr_name);
                if (leader_qp[i][qp_i] == NULL) {
                    usleep(200000);
                }
            }
            //  printf("main: Follower %d found Leader %d. Leader LID: %d\n",
            //  	l_id, i, leader_qp[i][qp_i]->lid);

            struct ibv_ah_attr ah_attr = {
              //-----INFINIBAND----------
              .is_global = 0,
              .dlid = (uint16_t) leader_qp[i][qp_i]->lid,
              .sl = leader_qp[i][qp_i]->sl,
              .src_path_bits = 0,
              /* port_num (> 1): device-local port for responses to this leader thread */
              .port_num = (uint8) (local_port_i + 1),
            };

            //  ---ROCE----------
            if (is_roce == 1) {
                ah_attr.is_global = 1;
                ah_attr.dlid = 0;
                ah_attr.grh.dgid.global.interface_id = leader_qp[i][qp_i]->gid_global_interface_id;
                ah_attr.grh.dgid.global.subnet_prefix = leader_qp[i][qp_i]->gid_global_subnet_prefix;
                ah_attr.grh.sgid_index = 0;
                ah_attr.grh.hop_limit = 1;
            }
            leader_ah[i][qp_i] = ibv_create_ah(cb->pd, &ah_attr);
            assert(leader_ah[i][qp_i] != NULL);
            remote_leader_qp[i][qp_i].ah = leader_ah[i][qp_i];
            remote_leader_qp[i][qp_i].qpn = leader_qp[i][qp_i]->qpn;
        }
    }
}

// Worker creates Ahs for the Client Qps that are used for Remote requests
void createAHs_for_worker(uint16_t wrkr_lid, struct hrd_ctrl_blk *cb) {
    int i, qp_i;
    struct ibv_ah *clt_ah[CLIENT_NUM][LEADER_QP_NUM];
    struct hrd_qp_attr *clt_qp[CLIENT_NUM][LEADER_QP_NUM];

    for(i = 0; i < CLIENT_NUM; i++) {
        if (i / LEADERS_PER_MACHINE == machine_id) continue; // skip the local clients
        /* Compute the control block and physical port index for client @i */
        int local_port_i = 0;

        char clt_name[QP_NAME_SIZE];
        sprintf(clt_name, "client-dgram-%d-%d", i, REMOTE_UD_QP_ID);
        /* Get the UD queue pair for the ith client */
        clt_qp[i][REMOTE_UD_QP_ID] = NULL;

        while(clt_qp[i][REMOTE_UD_QP_ID] == NULL) {
            clt_qp[i][REMOTE_UD_QP_ID] = hrd_get_published_qp(clt_name);
            //printf("Worker %d is expecting client %s\n" , wrkr_lid, clt_name);
            if(clt_qp[i][REMOTE_UD_QP_ID] == NULL) {
                usleep(200000);
            }
        }
        //  printf("main: Worker %d found client %d. Client LID: %d\n",
        //  	wrkr_lid, i, clt_qp[i][REMOTE_UD_QP_ID]->lid);

        struct ibv_ah_attr ah_attr = {
                //-----INFINIBAND----------
                .is_global = 0,
                .dlid = (uint16_t) clt_qp[i][REMOTE_UD_QP_ID]->lid,
                .sl = clt_qp[i][REMOTE_UD_QP_ID]->sl,
                .src_path_bits = 0,
                /* port_num (> 1): device-local port for responses to this client */
                .port_num = (uint8) (local_port_i + 1),
        };

        //  ---ROCE----------
        if (is_roce == 1) {
            ah_attr.is_global = 1;
            ah_attr.dlid = 0;
            ah_attr.grh.dgid.global.interface_id =  clt_qp[i][REMOTE_UD_QP_ID]->gid_global_interface_id;
            ah_attr.grh.dgid.global.subnet_prefix = clt_qp[i][REMOTE_UD_QP_ID]->gid_global_subnet_prefix;
            ah_attr.grh.sgid_index = 0;
            ah_attr.grh.hop_limit = 1;
        }
        clt_ah[i][REMOTE_UD_QP_ID] = ibv_create_ah(cb->pd, &ah_attr);
        assert(clt_ah[i][REMOTE_UD_QP_ID] != NULL);
        remote_leader_qp[i][REMOTE_UD_QP_ID].ah = clt_ah[i][REMOTE_UD_QP_ID];
        remote_leader_qp[i][REMOTE_UD_QP_ID].qpn = clt_qp[i][REMOTE_UD_QP_ID]->qpn;
    }
}

/* Generate a random permutation of [0, n - 1] for client @l_id */
int* get_random_permutation(int n, int clt_gid, uint64_t *seed) {
    int i, j, temp;
    assert(n > 0);

    /* Each client uses a different range in the cycle space of fastrand */
    for(i = 0; i < clt_gid * CACHE_NUM_KEYS; i++) {
        hrd_fastrand(seed);
    }

    printf("client %d: creating a permutation of 0--%d. This takes time..\n",
           clt_gid, n - 1);

    int *log = (int *) malloc(n * sizeof(int));
    assert(log != NULL);
    for(i = 0; i < n; i++) {
        log[i] = i;
    }

    printf("\tclient %d: shuffling..\n", clt_gid);
    for(i = n - 1; i >= 1; i--) {
        j = hrd_fastrand(seed) % (i + 1);
        temp = log[i];
        log[i] = log[j];
        log[j] = temp;
    }
    printf("\tclient %d: done creating random permutation\n", clt_gid);

    return log;
}

// Set up the buffer space of the worker for multiple qps: With M QPs per worker, Client X sends its reqs to QP: X mod M
void set_up_the_buffer_space(uint16_t clts_per_qp[], uint32_t per_qp_buf_slots[], uint32_t qp_buf_base[]) {
    int i, clt_i,qp = 0;
    // decide how many clients go to each QP
//    for (i = 0; i < LEADERS_PER_MACHINE; i++) {
//        assert(qp < FOLLOWER_QP_NUM);
//        clts_per_qp[qp]++;
//        MOD_ADD(qp, FOLLOWER_QP_NUM);
//    }
    for (i = 0; i < MACHINE_NUM; i++) {
        if (i == machine_id) continue;
        for (clt_i = 0; clt_i < LEADERS_PER_MACHINE; clt_i++) {
            clts_per_qp[(clt_i + i)% FOLLOWER_QP_NUM]++;
        }
    }
    qp_buf_base[0] = 0;
    for (i = 0; i < FOLLOWER_QP_NUM; i++) {
        per_qp_buf_slots[i] = clts_per_qp[i]  * WS_PER_WORKER;
//        cyan_printf("per_qp_buf_slots for qp %d : %d\n", i, per_qp_buf_slots[i]);
        if (i < FOLLOWER_QP_NUM - 1)
            qp_buf_base[i + 1] =  qp_buf_base[i] + per_qp_buf_slots[i];
    }
}

int parse_trace(char* path, struct trace_command **cmds, int clt_gid){
    FILE * fp;
    ssize_t read;
    size_t len = 0;
    char* ptr;
    char* word;
    char *saveptr;
    char* line = NULL;
    int i = 0;
    int cmd_count = 0;
    int word_count = 0;
    int letter_count = 0;
    int writes = 0;
    uint32_t hottest_key_counter = 0;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }

    while ((read = getline(&line, &len, fp)) != -1)
        cmd_count++;

    fclose(fp);
    if (line)
        free(line);

    len = 0;
    line = NULL;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }
    // printf("File %s has %d lines \n", path, cmd_count);
    (*cmds) = (struct trace_command *)malloc((cmd_count + 1) * sizeof(struct trace_command));
    int debug_cnt = 0;
    //parse file line by line and insert trace to cmd.
    for (i = 0; i < cmd_count; i++) {
        if ((read = getline(&line, &len, fp)) == -1)
            die("ERROR: Problem while reading the trace\n");
        word_count = 0;
        word = strtok_r (line, " ", &saveptr);
        (*cmds)[i].opcode = 0;

        //Before reading the request deside if it's gone be read or write
        uint8_t is_update = (rand() % 1000 < WRITE_RATIO) ? (uint8_t) 1 : (uint8_t) 0;
        if (is_update) {
            (*cmds)[i].opcode = (uint8_t) 1; // WRITE_OP
            writes++;
        }
        else  (*cmds)[i].opcode = (uint8_t) 0; // READ_OP

        while (word != NULL) {
            letter_count = 0;
            if (word[strlen(word) - 1] == '\n')
                word[strlen(word) - 1] = 0;

            if(word_count == 1) {
                (*cmds)[i].home_machine_id = (uint8_t) (strtoul(word, &ptr, 10) % MACHINE_NUM);
                if (RANDOM_MACHINE == 1) (*cmds)[i].home_machine_id = (uint8_t) (rand() % MACHINE_NUM);
                assert((*cmds)[i].home_machine_id < MACHINE_NUM);
                if (LOAD_BALANCE == 1){
                    (*cmds)[i].home_machine_id = (uint8_t) (rand() % MACHINE_NUM);
//                    printf("random %d \n", (*cmds)[i].home_machine_id);
                    while (DISABLE_LOCALS == 1 && (*cmds)[i].home_machine_id == machine_id)
                        (*cmds)[i].home_machine_id = (uint8_t) (rand() % MACHINE_NUM);
                }else if(DISABLE_LOCALS == 1 && (*cmds)[i].home_machine_id == (uint8_t) machine_id)
                    (*cmds)[i].home_machine_id = (uint8_t) (((*cmds)[i].home_machine_id + 1) % MACHINE_NUM);
                if (SEND_ONLY_TO_ONE_MACHINE == 1)
                    if(DISABLE_LOCALS == 1 && machine_id == 0)
                        (*cmds)[i].home_machine_id = (uint8_t) 1;
                    else
                        (*cmds)[i].home_machine_id = (uint8_t) 0;
                else if(SEND_ONLY_TO_NEXT_MACHINE == 1)
                    (*cmds)[i].home_machine_id = (uint8_t) ((machine_id + 1) % MACHINE_NUM);
                else if(BALANCE_REQS_IN_CHUNKS == 1)
                    (*cmds)[i].home_machine_id = (uint8_t) (CHUNK_NUM == 0? 0 : (i / CHUNK_NUM) % MACHINE_NUM);
                else if (DO_ONLY_LOCALS == 1) (*cmds)[i].home_machine_id = (uint8) machine_id;
                assert(DISABLE_LOCALS == 0 || machine_id != (*cmds)[i].home_machine_id);
            } else if(word_count == 2){
                (*cmds)[i].home_worker_id = (uint8_t) (strtoul(word, &ptr, 10) % FOLLOWERS_PER_MACHINE);
                if(LOAD_BALANCE == 1 || EMULATING_CREW == 1){
                    (*cmds)[i].home_worker_id = (uint8_t) (rand() % ACTIVE_WORKERS_PER_MACHINE );
                }
                assert((*cmds)[i].home_worker_id < FOLLOWERS_PER_MACHINE);
            } else if(word_count == 3){
                (*cmds)[i].key_id = (uint32_t) strtoul(word, &ptr, 10);
                if (ONLY_CACHE_HITS == 1)
                    (*cmds)[i].key_id = (uint32) rand() % CACHE_NUM_KEYS;
                // HOT KEYS
                if ((*cmds)[i].key_id < CACHE_NUM_KEYS) {
                    if ((BALANCE_HOT_WRITES == 1 && is_update) || BALANCE_HOT_REQS == 1) {
                        (*cmds)[i].key_id = (uint32_t) rand() % CACHE_NUM_KEYS;
                        range_assert((*cmds)[i].key_id, 0, CACHE_NUM_KEYS);
                    }
                    else if (ENABLE_HOT_REQ_GROUPING == 1) {
                        if ((*cmds)[i].key_id < NUM_OF_KEYS_TO_GROUP) {
                            (*cmds)[i].key_id = ((*cmds)[i].key_id * GROUP_SIZE) + (rand() % GROUP_SIZE);
                        }
                        else if ((*cmds)[i].key_id < NUM_OF_KEYS_TO_GROUP * GROUP_SIZE)
                            (*cmds)[i].key_id += GROUP_SIZE;
                    }
                    if ((*cmds)[i].key_id < HOTTEST_KEYS_TO_TRACK) hottest_key_counter++;
                }
                else { // COLD KEYS
                    (*cmds)[i].key_id %= HERD_NUM_KEYS;
                    if ((*cmds)[i].key_id < CACHE_NUM_KEYS) (*cmds)[i].key_id+= CACHE_NUM_KEYS;
                }
                if(USE_A_SINGLE_KEY == 1)
                    (*cmds)[i].key_id =  0;
                if ((*cmds)[i].key_id < CACHE_NUM_KEYS) { // hot
                    if ((*cmds)[i].opcode == 1) // hot write
                        (*cmds)[i].opcode = (uint8_t) HOT_WRITE;
                    else (*cmds)[i].opcode = (uint8_t) HOT_READ; // hot read
                }
                else {
                    if ((*cmds)[i].opcode == 1) { // normal write
                        if ((*cmds)[i].home_machine_id == machine_id)
                            (*cmds)[i].opcode = (uint8_t) LOCAL_WRITE;
                        else (*cmds)[i].opcode = (uint8_t) REMOTE_WRITE;
                    }
                    else { // normal read
                        if ((*cmds)[i].home_machine_id == machine_id)
                            (*cmds)[i].opcode = (uint8_t) LOCAL_READ;
                        else (*cmds)[i].opcode = (uint8_t) REMOTE_READ;
                    }
                }
                (*cmds)[i].key_hash = CityHash128((char *) &((*cmds)[i].key_id), 4);


                debug_cnt++;
            }else if(word_count == 0) {
                while (word[letter_count] != '\0'){
                    switch(word[letter_count]) {
                        // case 'H' :
                        //     (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | HOT_KEY);
                        //     break;
                        // case 'N' :
                        //     (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | NORMAL_KEY);
                        //     break;
                        //     case 'R' :
                        //         (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | READ_OP);
                        //         break;
                        //     case 'W' :
                        //         (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | WRITE_OP);
                        //         break;
                        default :
                            break;
                            assert(0);
                    }
                    letter_count++;
                }
            }

            word_count++;
            word = strtok_r(NULL, " ", &saveptr);
            if (word == NULL && word_count < 4) {
                printf("Client %d Error: Reached word %d in line %d : %s \n",clt_gid, word_count, i, line);
                assert(false);
            }
        }

    }
    if (clt_gid  == 0) printf("Write Ratio: %.2f%% \n", (double) (writes * 100) / cmd_count);
    if (clt_gid  == 0) printf("Hottest keys percentage of the trace: %.2f%% for %d keys \n",
                              (double) (hottest_key_counter * 100) / cmd_count, HOTTEST_KEYS_TO_TRACK);
    (*cmds)[cmd_count].opcode = NOP;
    // printf("CLient %d Trace size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
    //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
    assert(cmd_count == debug_cnt);
    fclose(fp);
    if (line)
        free(line);
    return cmd_count;
}


// Manufactures a trace without a file
void manufacture_trace(struct trace_command **cmds, int g_id)
{
  (*cmds) = (struct trace_command *)malloc((TRACE_SIZE + 1) * sizeof(struct trace_command));

  uint32_t i, writes = 0;
  //parse file line by line and insert trace to cmd.
  for (i = 0; i < TRACE_SIZE; i++) {
    (*cmds)[i].opcode = 0;

    //Before reading the request deside if it's gone be read or write
    uint8_t is_update = (rand() % 1000 < WRITE_RATIO) ? (uint8_t) 1 : (uint8_t) 0;
    if (is_update) {
      (*cmds)[i].opcode = (uint8_t) 1; // WRITE_OP

    }
    else  (*cmds)[i].opcode = (uint8_t) 2; // READ_OP

    if (FOLLOWER_DOES_ONLY_READS && (!is_leader)) (*cmds)[i].opcode = (uint8_t) 2;

    //--- KEY ID----------
    (*cmds)[i].key_id = (uint32) rand() % CACHE_NUM_KEYS;
    if(USE_A_SINGLE_KEY == 1) (*cmds)[i].key_id =  0;
    (*cmds)[i].key_hash = CityHash128((char *) &((*cmds)[i].key_id), 4);

    if ((*cmds)[i].opcode == 1) writes++;
  }

  if (g_id  == 0) printf("Write Ratio: %.2f%% \n, Trace size %d \n", (double) (writes * 100) / TRACE_SIZE, TRACE_SIZE);
  (*cmds)[TRACE_SIZE].opcode = NOP;
  // printf("CLient %d Trace size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
  //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
}

void trace_init(struct trace_command **cmds, int g_id) {
    //create the trace path path
    if (FEED_FROM_TRACE == 1) {
        char local_client_id[3];
        char machine_num[4];
        //get / creat path for the trace
        if (LEADERS_PER_MACHINE <= 23) sprintf(local_client_id, "%d", (g_id % LEADERS_PER_MACHINE));
        else  sprintf(local_client_id, "%d", (g_id % 23));// the traces are for 8 clients
        // sprintf(local_client_id, "%d", (l_id % 4));
        sprintf(machine_num, "%d", machine_id);
        char path[2048];
        char cwd[1024];
        char *was_successful = getcwd(cwd, sizeof(cwd));

        if (!was_successful) {
            printf("ERROR: getcwd failed!\n");
            exit(EXIT_FAILURE);
        }

        snprintf(path, sizeof(path), "%s%s%s%s%s%s%d%s", cwd,
                 "/../../traces/current-splited-traces/s_",
                 machine_num, "_c_", local_client_id, "_a_", SKEW_EXPONENT_A, ".txt");
        //initialize the command array from the trace file
        // printf("Thread: %d attempts to read the trace: %s\n", l_id, path);
        parse_trace(path, cmds, g_id % LEADERS_PER_MACHINE);
        //printf("Trace read by client: %d\n", l_id);
    }else {
      manufacture_trace(cmds, g_id);
    }

}

void dump_stats_2_file(struct stats* st){
    uint8_t typeNo = protocol;
    assert(typeNo >=0 && typeNo <=3);
    int i = 0;
    char filename[128];
    FILE *fp;
    double total_MIOPS;
    char* path = "../../results/scattered-results/";
    const char * exectype[] = {
            "BS", //baseline
            "SC", //Sequential Consistency
            "LIN", //Linearizability (non stalling)
            "SS" //Strong Consistency (stalling)
    };

    sprintf(filename, "%s/%s_%s_%s_s_%d_a_%d_v_%d_m_%d_c_%d_w_%d_r_%d%s-%d.csv", path,
            DISABLE_CACHE == 1 ? "BS" : exectype[typeNo],
            LOAD_BALANCE == 1 ? "UNIF" : "SKEW",
            (ENABLE_WORKERS_CRCW == 1 ? "CRCW" : (EMULATING_CREW == 1 ? "CREW" : "EREW")),
            DISABLE_CACHE == 0 && typeNo == 2 && ENABLE_MULTIPLE_SESSIONS != 0 && SESSIONS_PER_THREAD != 0 ? SESSIONS_PER_THREAD: 0,
            SKEW_EXPONENT_A,
            USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
            MACHINE_NUM, LEADERS_PER_MACHINE,
            FOLLOWERS_PER_MACHINE, WRITE_RATIO,
            BALANCE_HOT_WRITES == 1  ? "_lbw" : "",
            machine_id);
    printf("%s\n", filename);
    fp = fopen(filename, "w"); // "w" means that we are going to write on this file
    fprintf(fp, "machine_id: %d\n", machine_id);
    fprintf(fp, "comment: worker ID, total MIOPS, local MIOPS, remote MIOPS\n");

    for(i = 0; i < FOLLOWERS_PER_MACHINE; ++i){
        total_MIOPS = st->locals_per_worker[i] + st->remotes_per_worker[i];
        fprintf(fp, "worker: %d, %.2f, %.2f, %.2f\n", i, total_MIOPS,
                st->locals_per_worker[i], st->remotes_per_worker[i]);
    }

    fprintf(fp, "comment: client ID, total MIOPS, cache MIOPS, local MIOPS,"
            "remote MIOPS, updates, invalidates, acks, received updates,"
            "received invalidates, received acks\n");
    for(i = 0; i < LEADERS_PER_MACHINE; ++i){
        total_MIOPS = st->cache_hits_per_client[i] +
                      st->locals_per_client[i] + st->remotes_per_client[i];
        fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
                i, total_MIOPS, st->cache_hits_per_client[i], st->locals_per_client[i],
                st->remotes_per_client[i], st->updates_per_client[i],
                st->invs_per_client[i], st->acks_per_client[i],
                st->received_updates_per_client[i], st->received_invs_per_client[i],
                st->received_acks_per_client[i]);
    }

    /*
    fprintf(fp, "comment: cache MIOPS\n");
    fprintf(fp, "cache: %.2f\n", cache_MIOPS);
    machine_MIOPS += cache_MIOPS;
    fprintf(fp, "comment: machine MIOPS\n");
    fprintf(fp, "machine: %.2f\n", machine_MIOPS);
    */
    fclose(fp);
}

void append_throughput(double throughput)
{
    FILE *throughput_fd;
    throughput_fd = fopen("../../results/throughput.txt", "a");
    fprintf(throughput_fd, "%2.f \n", throughput);
    fclose(throughput_fd);
}

int spawn_stats_thread() {
    pthread_t *thread_arr = malloc(sizeof(pthread_t));
    pthread_attr_t attr;
    cpu_set_t cpus_stats;
    int core = -1;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpus_stats);
    if(num_threads > 17) {
        core = 39;
        CPU_SET(core, &cpus_stats);
    }
    else {
        core = 2 * (num_threads) + 2;
        CPU_SET(core, &cpus_stats);
    }
    yellow_printf("Creating stats thread at core %d\n", core);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
    return pthread_create(&thread_arr[0], &attr, print_stats, NULL);
}

// pin threads starting from core 0
int pin_thread(int t_id) {
    int core;
    core = PHYSICAL_CORE_DISTANCE * t_id;
    if(core > TOTAL_CORES_) { //if you run out of cores in numa node 0
        if (WORKER_HYPERTHREADING) { //use hyperthreading rather than go to the other socket
            core = PHYSICAL_CORE_DISTANCE * (t_id - PHYSICAL_CORES_PER_SOCKET) + 2;
        }
        else { //spawn clients to numa node 1
            core = (t_id - PHYSICAL_CORES_PER_SOCKET) * PHYSICAL_CORE_DISTANCE + 1;
        }
    }
    assert(core >= 0 && core < TOTAL_CORES);
    return core;
}

// pin a thread avoid collisions with pin_thread()
int pin_threads_avoiding_collisions(int c_id) {
    int c_core;
    if (!WORKER_HYPERTHREADING || FOLLOWERS_PER_MACHINE < PHYSICAL_CORES_PER_SOCKET) {
        if (c_id < FOLLOWERS_PER_MACHINE) c_core = PHYSICAL_CORE_DISTANCE * c_id + 2;
        else c_core = (FOLLOWERS_PER_MACHINE * 2) + (c_id * 2);

        //if (DISABLE_CACHE == 1) c_core = 4 * i + 2; // when bypassing the cache
        //if (DISABLE_HYPERTHREADING == 1) c_core = (FOLLOWERS_PER_MACHINE * 4) + (c_id * 4);
        if (c_core > TOTAL_CORES_) { //spawn clients to numa node 1 if you run out of cores in 0
            c_core -= TOTAL_CORES_;
        }
    }
    else { //we are keeping workers on the same socket
        c_core = (FOLLOWERS_PER_MACHINE - PHYSICAL_CORES_PER_SOCKET) * 4 + 2 + (4 * c_id);
        if (c_core > TOTAL_CORES_) c_core = c_core - (TOTAL_CORES_ + 2);
        if (c_core > TOTAL_CORES_) c_core = c_core - (TOTAL_CORES_ - 1);
    }
    assert(c_core >= 0 && c_core < TOTAL_CORES);
    return c_core;
}


/* ---------------------------------------------------------------------------
------------------------------CLIENT INITIALIZATION --------------------------
---------------------------------------------------------------------------*/
// Post receives for the coherence traffic in the init phase
void post_coh_recvs(struct hrd_ctrl_blk *cb, int* push_ptr, struct mcast_essentials *mcast, int protocol, void* buf)
{
    check_protocol(protocol);
    int i, j;
    int credits = protocol == FOLLOWER ? SC_CREDITS : BROADCAST_CREDITS;
    int max_reqs = protocol == FOLLOWER ? SC_CLT_BUF_SLOTS : LIN_CLT_BUF_SLOTS;
    for(i = 0; i < MACHINE_NUM - 1; i++) {
        for(j = 0; j < credits; j++) {
            if (ENABLE_MULTICAST == 1) {
                hrd_post_dgram_recv(mcast->recv_qp,	(void *) (buf + *push_ptr * UD_REQ_SIZE),
                                    UD_REQ_SIZE, mcast->recv_mr->lkey);
            }
            else hrd_post_dgram_recv(cb->dgram_qp[BROADCAST_UD_QP_ID],
                                     (void *) (buf + *push_ptr * UD_REQ_SIZE), UD_REQ_SIZE, cb->dgram_buf_mr->lkey);
            MOD_ADD(*push_ptr, max_reqs);
            //if (*push_ptr == 0) *push_ptr = 1;
        }
    }
}

// Initialize the mcast_essentials structure that is necessary
void init_multicast(struct mcast_info **mcast_data, struct mcast_essentials **mcast,
                    int local_client_id, struct hrd_ctrl_blk *cb, int protocol)
{
    check_protocol(protocol);
    uint16_t remote_buf_size =  ENABLE_WORKER_COALESCING == 1 ?
                                (GRH_SIZE + sizeof(struct wrkr_coalesce_mica_op)) : UD_REQ_SIZE;
    size_t dgram_buf_size = (size_t) (protocol == FOLLOWER ? SC_CLT_BUF_SIZE + remote_buf_size : LIN_CLT_BUF_SIZE + remote_buf_size);
    int recv_q_depth = protocol == FOLLOWER ? SC_CLIENT_RECV_BR_Q_DEPTH : LIN_CLIENT_RECV_BR_Q_DEPTH;
    *mcast_data = malloc(sizeof(struct mcast_info));
    (*mcast_data)->clt_id = local_client_id;
    setup_multicast(*mcast_data, recv_q_depth);
    // char buf[40];
    // inet_ntop(AF_INET6, (*mcast_data)->mcast_ud_param.ah_attr.grh.dgid.raw, buf, 40);
    // printf("client: joined dgid: %s mlid 0x%x sl %d\n", buf,	(*mcast_data)->mcast_ud_param.ah_attr.dlid, (*mcast_data)->mcast_ud_param.ah_attr.sl);
    *mcast = malloc(sizeof(struct mcast_essentials));
    (*mcast)->send_ah = ibv_create_ah(cb->pd, &((*mcast_data)->mcast_ud_param.ah_attr));
    (*mcast)->qpn  =  (*mcast_data)->mcast_ud_param.qp_num;
    (*mcast)->qkey  =  (*mcast_data)->mcast_ud_param.qkey;
    (*mcast)->recv_cq = (*mcast_data)->cm_qp[RECV_MCAST_QP].cq;
    (*mcast)->recv_qp = (*mcast_data)->cm_qp[RECV_MCAST_QP].cma_id->qp;
    (*mcast)->recv_mr = ibv_reg_mr((*mcast_data)->cm_qp[RECV_MCAST_QP].pd, (void*) cb->dgram_buf,
                                   dgram_buf_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                                   IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    free(*mcast_data);
    assert((*mcast)->recv_mr != NULL);
}

// set the different queue depths for client's queue pairs
void set_up_queue_depths(int** recv_q_depths, int** send_q_depths, int protocol)
{
    /* 1st Dgram for communication between Clients and servers
      2nd Dgram for Broadcasting
      3rd Dgram for Flow Control (Credit-based) */
    if (protocol == FOLLOWER) {
      *send_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
      *recv_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
      (*recv_q_depths)[REMOTE_UD_QP_ID] = CLIENT_RECV_REM_Q_DEPTH;
      (*recv_q_depths)[BROADCAST_UD_QP_ID] = ENABLE_MULTICAST == 1 ? 1 : SC_CLIENT_RECV_BR_Q_DEPTH;
      (*recv_q_depths)[FC_UD_QP_ID] = SC_CLIENT_RECV_CR_Q_DEPTH;
      (*send_q_depths)[REMOTE_UD_QP_ID] = CLIENT_SEND_REM_Q_DEPTH;
      (*send_q_depths)[BROADCAST_UD_QP_ID] = SC_CLIENT_SEND_BR_Q_DEPTH;
      (*send_q_depths)[FC_UD_QP_ID] = SC_CLIENT_SEND_CR_Q_DEPTH;
    }
    else if (protocol == LEADER) {
      *send_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
      *recv_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
      (*recv_q_depths)[REMOTE_UD_QP_ID] = CLIENT_RECV_REM_Q_DEPTH;
      (*recv_q_depths)[BROADCAST_UD_QP_ID] = LIN_CLIENT_RECV_BR_Q_DEPTH;
      (*recv_q_depths)[FC_UD_QP_ID] = LIN_CLIENT_RECV_CR_Q_DEPTH;
      (*send_q_depths)[REMOTE_UD_QP_ID] = CLIENT_SEND_REM_Q_DEPTH;
      (*send_q_depths)[BROADCAST_UD_QP_ID] = LIN_CLIENT_SEND_BR_Q_DEPTH;
      (*send_q_depths)[FC_UD_QP_ID] = LIN_CLIENT_SEND_CR_Q_DEPTH;
    }
    else check_protocol(protocol);
}

// Used by all kinds of threads to publish their QPs
void publish_qps(uint32_t qp_num, uint32_t global_id, const char* qp_name, struct hrd_ctrl_blk *cb)
{
  uint32_t qp_i;
  for (qp_i = 0; qp_i < qp_num; qp_i++) {
    char dgram_qp_name[QP_NAME_SIZE];
    sprintf(dgram_qp_name, "%s-%d-%d", qp_name, global_id, qp_i);
    hrd_publish_dgram_qp(cb, qp_i, dgram_qp_name, DEFAULT_SL);
//    printf("Thread %d published dgram %s \n", local_id, dgram_qp_name);
  }
}

// Followers and leaders both use this to establish connections
void setup_connections_and_spawn_stats_thread(int global_id, struct hrd_ctrl_blk *cb)
{
    int qp_i;
    int t_id = -1;
    char dgram_qp_name[QP_NAME_SIZE];
    if (is_leader) {
      t_id = global_id;
      publish_qps(LEADER_QP_NUM, global_id, "leader-dgram", cb);
    }
    else {
      t_id = global_id % FOLLOWERS_PER_MACHINE;
      publish_qps(FOLLOWER_QP_NUM, global_id, "follower-dgram", cb);
    }
    if (t_id == 0) {
      if (is_leader) get_qps_from_all_other_machines(global_id, cb);
      else get_qps_from_one_machine(global_id, cb);
      assert(qps_are_set_up == 0);
      // Spawn a thread that prints the stats
      if (spawn_stats_thread() != 0)
          red_printf("Stats thread was not successfully spawned \n");
      atomic_store_explicit(&qps_are_set_up, 1, memory_order_release);
    }
    else {
        while (atomic_load_explicit(&qps_are_set_up, memory_order_acquire)== 0);  usleep(200000);
    }
    assert(qps_are_set_up == 1);
//    printf("Thread %d has all the needed ahs\n", global_id );
}

// set up the OPS buffers
void set_up_ops(struct extended_cache_op **ops, struct extended_cache_op **next_ops, struct extended_cache_op **third_ops,
                struct mica_resp **resp, struct mica_resp **next_resp, struct mica_resp **third_resp,
                struct key_home **key_homes, struct key_home **next_key_homes, struct key_home **third_key_homes)
{
    int i;
    uint32_t extended_ops_size = (OPS_BUFS_NUM * CACHE_BATCH_SIZE * (sizeof(struct extended_cache_op)));
    *ops = memalign(4096, extended_ops_size);
    memset(*ops, 0, extended_ops_size);
    *next_ops = &((*ops)[CACHE_BATCH_SIZE]);
    *third_ops = &((*ops)[2 * CACHE_BATCH_SIZE]); //only used when no Inlining happens


    *resp = memalign(4096, OPS_BUFS_NUM * CACHE_BATCH_SIZE * sizeof(struct mica_resp));
    *next_resp = &((*resp)[CACHE_BATCH_SIZE]);
    *third_resp = &((*resp)[2 * CACHE_BATCH_SIZE]);

    *key_homes = memalign(4096, OPS_BUFS_NUM * CACHE_BATCH_SIZE * sizeof(struct key_home));
    *next_key_homes = &((*key_homes)[CACHE_BATCH_SIZE]);
    *third_key_homes = &((*key_homes)[2 * CACHE_BATCH_SIZE]);

    assert(ops != NULL && next_ops != NULL && third_ops != NULL &&
           resp != NULL && next_resp != NULL && third_resp != NULL &&
           key_homes != NULL && next_key_homes != NULL && third_key_homes != NULL);

    for(i = 0; i <  OPS_BUFS_NUM * CACHE_BATCH_SIZE; i++)
        (*resp)[i].type = EMPTY;
}

// set up the coherence buffers
void set_up_coh_ops(struct cache_op **update_ops, struct cache_op **ack_bcast_ops, struct small_cache_op **inv_ops,
                    struct small_cache_op **inv_to_send_ops, struct mica_resp *update_resp, struct mica_resp *inv_resp,
                    struct mica_op **coh_buf, int protocol)
{
    check_protocol(protocol);
    int i;
    *coh_buf = memalign(4096, COH_BUF_SIZE);
    uint16_t cache_op_size = sizeof(struct cache_op);
    uint16_t small_cache_op_size = sizeof(struct small_cache_op);
    *update_ops = (struct cache_op *)malloc(BCAST_TO_CACHE_BATCH * cache_op_size); /* Batch of incoming broadcasts for the Cache*/
    if (protocol != FOLLOWER) {
        *ack_bcast_ops = (struct cache_op *)malloc(BCAST_TO_CACHE_BATCH * cache_op_size);
        *inv_ops = (struct small_cache_op *)malloc(BCAST_TO_CACHE_BATCH * small_cache_op_size);
        *inv_to_send_ops = (struct small_cache_op *)malloc(BCAST_TO_CACHE_BATCH * small_cache_op_size);
    }
    assert(*update_ops != NULL);
    if (protocol != FOLLOWER)
        assert(*ack_bcast_ops != NULL && *inv_ops != NULL && *inv_to_send_ops != NULL);
    for(i = 0; i < BCAST_TO_CACHE_BATCH; i++) {
        update_resp[i].type = EMPTY;
        if (protocol != FOLLOWER) {
            inv_resp[i].type = EMPTY;
            (*inv_to_send_ops)[i].opcode = EMPTY;
        }
    }

}

// Set up the memory registrations required in the client if there is no Inlining
void set_up_mrs(struct ibv_mr **ops_mr, struct ibv_mr **coh_mr, struct extended_cache_op* ops,
                struct mica_op *coh_buf, struct hrd_ctrl_blk *cb)
{
    if (LEADER_ENABLE_INLINING == 0) {
        uint32_t extended_ops_size = (OPS_BUFS_NUM * CACHE_BATCH_SIZE * (sizeof(struct extended_cache_op)));
        *ops_mr = register_buffer(cb->pd, (void*)ops, extended_ops_size);
        if (WRITE_RATIO != 0) *coh_mr = register_buffer(cb->pd, (void*)coh_buf, COH_BUF_SIZE);
    }
}



// Set up the remote Requests send and recv WRs
void set_up_remote_WRs(struct ibv_send_wr* rem_send_wr, struct ibv_sge* rem_send_sgl,
                       struct ibv_recv_wr* rem_recv_wr, struct ibv_sge* rem_recv_sgl,
                       struct hrd_ctrl_blk *cb, int clt_gid, struct ibv_mr* ops_mr, int protocol)
{
    int i;
    check_protocol(protocol);
    uint16_t remote_buf_size = ENABLE_WORKER_COALESCING == 1 ?
                               (GRH_SIZE + sizeof(struct wrkr_coalesce_mica_op)) : UD_REQ_SIZE ;
    // This should be same for both protocols
    for (i = 0; i < WINDOW_SIZE; i++) {
        if (LEADER_ENABLE_INLINING == 0) rem_send_sgl[i].lkey = ops_mr->lkey;
        else rem_send_wr[i].send_flags = IBV_SEND_INLINE;
        rem_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        rem_send_wr[i].imm_data = (uint32) clt_gid;
        rem_send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
        rem_send_wr[i].num_sge = 1;
        rem_send_wr[i].sg_list = &rem_send_sgl[i];
        if (USE_ONLY_BIG_MESSAGES == 1)
            rem_recv_sgl->length = HERD_PUT_REQ_SIZE + sizeof(struct ibv_grh);
        else
            rem_recv_sgl->length = remote_buf_size;
        rem_recv_sgl->lkey = cb->dgram_buf_mr->lkey;
        rem_recv_sgl->addr = (uintptr_t) &cb->dgram_buf[0];
        rem_recv_wr[i].sg_list = rem_recv_sgl;
        rem_recv_wr[i].num_sge = 1;
    }
}

// Set up all coherence WRs
void set_up_coh_WRs(struct ibv_send_wr *coh_send_wr, struct ibv_sge *coh_send_sgl,
                    struct ibv_recv_wr *coh_recv_wr, struct ibv_sge *coh_recv_sgl,
                    struct ibv_send_wr *ack_wr, struct ibv_sge *ack_sgl,
                    struct mica_op *coh_buf, uint16_t local_client_id,
                    struct hrd_ctrl_blk *cb, struct ibv_mr *coh_mr, struct mcast_essentials *mcast, int protocol)
{
    int i, j;
    check_protocol(protocol);
    //BROADCAST WRs and credit Receives
    for (j = 0; j < MAX_BCAST_BATCH; j++) {
        //coh_send_sgl[j].addr = (uint64_t) (uintptr_t) (coh_buf + j);
        if (LEADER_ENABLE_INLINING == 0) coh_send_sgl[j].lkey = coh_mr->lkey;
        for (i = 0; i < MESSAGES_IN_BCAST; i++) {
            uint16_t rm_id;
            if (i < machine_id) rm_id = (uint16_t) i;
            else rm_id = (uint16_t) ((i + 1) % MACHINE_NUM);
            uint16_t clt_i = (uint16_t) (rm_id * LEADERS_PER_MACHINE + local_client_id);
            uint16_t index = (uint16_t) ((j * MESSAGES_IN_BCAST) + i);
            assert (index < MESSAGES_IN_BCAST_BATCH);
            if (ENABLE_MULTICAST == 1) {
                coh_send_wr[index].wr.ud.ah = mcast->send_ah;
                coh_send_wr[index].wr.ud.remote_qpn = mcast->qpn;
                coh_send_wr[index].wr.ud.remote_qkey = mcast->qkey;
            } else {
                if (protocol == FOLLOWER) {
                  coh_send_wr[index].wr.ud.ah = remote_leader_qp[clt_i][BROADCAST_UD_QP_ID].ah;
                  coh_send_wr[index].wr.ud.remote_qpn = (uint32) remote_leader_qp[clt_i][BROADCAST_UD_QP_ID].qpn;
                }
                else {
//                  coh_send_wr[index].wr.ud.ah = remote_follower_qp[i][][BROADCAST_UD_QP_ID].ah;
//                  coh_send_wr[index].wr.ud.remote_qpn = (uint32) remote_follower_qp[clt_i][][BROADCAST_UD_QP_ID].qpn;
                }
              coh_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
            }
            if (protocol == FOLLOWER) coh_send_wr[index].opcode = IBV_WR_SEND_WITH_IMM; // TODO we should remove imms from here too
            else coh_send_wr[index].opcode = IBV_WR_SEND; // Attention!! there is no immediate here, cids do the job!
            coh_send_wr[index].num_sge = 1;
            coh_send_wr[index].sg_list = &coh_send_sgl[j];
            if (protocol == FOLLOWER) coh_send_wr[index].imm_data = (uint32) machine_id;
            if (LEADER_ENABLE_INLINING == 1) coh_send_wr[index].send_flags = IBV_SEND_INLINE;
            coh_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &coh_send_wr[index + 1];
        }
    }

    // Coherence Receives
    int max_coh_receives = protocol == FOLLOWER ? SC_MAX_COH_RECEIVES : MAX_COH_RECEIVES;
    for (i = 0; i < max_coh_receives; i++) {
        coh_recv_sgl[i].length = UD_REQ_SIZE;
        if (protocol == FOLLOWER && ENABLE_MULTICAST == 1)
            coh_recv_sgl[i].lkey = mcast->recv_mr->lkey;
        else  coh_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        coh_recv_wr[i].sg_list = &coh_recv_sgl[i];
        coh_recv_wr[i].num_sge = 1;
    }

    // Do acknowledgements
    if (protocol == LEADER) {
        // ACK WRs
        for (i = 0; i < BCAST_TO_CACHE_BATCH; i++) {
            ack_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
            //ack_wr[i].imm_data = machine_id;
            ack_sgl[i].length = HERD_GET_REQ_SIZE;
            ack_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no immediate here, cids do the job!
            ack_wr[i].num_sge = 1;
            ack_wr[i].sg_list = &ack_sgl[i];
        }
    }
}

void set_up_credits(uint8_t credits[][MACHINE_NUM], struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                    struct ibv_recv_wr* credit_recv_wr, struct ibv_sge* credit_recv_sgl,
                    struct hrd_ctrl_blk *cb, int protocol)
{
    check_protocol(protocol);
    int i = 0;
    int max_credt_wrs = protocol == FOLLOWER ?  SC_MAX_CREDIT_WRS : MAX_CREDIT_WRS;
    int max_credit_recvs = protocol == FOLLOWER ? SC_MAX_CREDIT_RECVS : MAX_CREDIT_RECVS;
    // Credits
    if (protocol == FOLLOWER)
        for (i = 0; i < MACHINE_NUM; i++) credits[SC_UPD_VC][i] = W_CREDITS;
    else {
        for (i = 0; i < FOLLOWER_MACHINE_NUM; i++) {
            credits[PREP_VC][i] = PREPARE_CREDITS;
            credits[COMM_VC][i] = COMMIT_CREDITS;
        }
    }
    // Credit WRs
    for (i = 0; i < max_credt_wrs; i++) {
        credit_send_sgl->length = 0;
        credit_send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
        credit_send_wr[i].num_sge = 0;
        credit_send_wr[i].sg_list = credit_send_sgl;
        if (protocol == FOLLOWER) credit_send_wr[i].imm_data = (uint32) machine_id;
        credit_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        credit_send_wr[i].next = NULL;
        credit_send_wr[i].send_flags = IBV_SEND_INLINE;
    }
    //Credit Receives
    credit_recv_sgl->length = 64;
    credit_recv_sgl->lkey = cb->dgram_buf_mr->lkey;
    credit_recv_sgl->addr = (uintptr_t) &cb->dgram_buf[0];
    for (i = 0; i < max_credit_recvs; i++) {
        credit_recv_wr[i].sg_list = credit_recv_sgl;
        credit_recv_wr[i].num_sge = 1;
    }
}


/* ---------------------------------------------------------------------------
------------------------------WORKER INITIALIZATION --------------------------
---------------------------------------------------------------------------*/

void set_up_wrs(struct wrkr_coalesce_mica_op** response_buffer, struct ibv_mr* resp_mr,
                struct hrd_ctrl_blk *cb, struct ibv_sge* recv_sgl,
                struct ibv_recv_wr* recv_wr, struct ibv_send_wr* wr, struct ibv_sge* sgl, uint16_t wrkr_lid)
{
    uint16_t i;
    if ((WORKER_ENABLE_INLINING == 0) || (ENABLE_WORKER_COALESCING == 1)) {
        uint32_t resp_buf_size = ENABLE_WORKER_COALESCING == 1 ? sizeof(struct wrkr_coalesce_mica_op)* WORKER_SS_BATCH :
                                 sizeof(struct mica_op)* WORKER_SS_BATCH; //the buffer needs to be large enough to deal with NIC asynchronous reads
        *response_buffer = malloc(resp_buf_size);
        resp_mr = register_buffer(cb->pd, (void*)(*response_buffer), resp_buf_size);
    }

    // Initialize the Work requests and the Receive requests
    for (i = 0; i < WORKER_MAX_BATCH; i++) {
        if (!ENABLE_COALESCING)
            recv_sgl[i].length = HERD_PUT_REQ_SIZE + sizeof(struct ibv_grh);//req_size;
        else recv_sgl[i].length = sizeof(struct wrkr_ud_req);
        recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        recv_wr[i].sg_list = &recv_sgl[i];
        recv_wr[i].num_sge = 1;


        wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        wr[i].opcode = IBV_WR_SEND; // Immediate is not used here
        if (WORKER_ENABLE_INLINING == 0) {
            sgl[i].lkey = resp_mr->lkey;
            // sgl[i].addr = (uintptr_t)(*response_buffer)[i].value;
        }
        if(ENABLE_MULTI_BATCHES) //) || MEASURE_LATENCY)
            wr[i].opcode = IBV_WR_SEND_WITH_IMM; // Immediate is not used here
        wr[i].num_sge = 1;
        wr[i].sg_list = &sgl[i];
        if(ENABLE_MULTI_BATCHES || MEASURE_LATENCY)
            wr[i].imm_data = (uint32) (machine_id * FOLLOWERS_PER_MACHINE) + wrkr_lid;
    }
}

/* ---------------------------------------------------------------------------
------------------------------LEADER --------------------------------------
---------------------------------------------------------------------------*/
// construct a prep_message-- max_size must be in bytes
void init_fifo(struct fifo **fifo, uint32_t max_size, uint32_t fifos_num)
{
  (*fifo) = (struct fifo *)malloc(fifos_num * sizeof(struct fifo));
  memset((*fifo), 0, fifos_num *  sizeof(struct fifo));
  for (int i = 0; i < fifos_num; ++i) {
    fifo[i]->fifo = malloc(max_size);
    memset(fifo[i]->fifo, 0, max_size);
  }


}


// Set up the receive info
void init_recv_info(struct recv_info **recv, uint32_t push_ptr, uint32_t buf_slots,
                    uint32_t slot_size, uint32_t posted_recvs, struct ibv_recv_wr *recv_wr,
                    struct ibv_qp * recv_qp, struct ibv_sge* recv_sgl, void* buf)
{
  (*recv) = malloc(sizeof(struct recv_info));
  (*recv)->push_ptr = push_ptr;
  (*recv)->buf_slots = buf_slots;
  (*recv)->slot_size = slot_size;
  (*recv)->posted_recvs = posted_recvs;
  (*recv)->recv_wr = recv_wr;
  (*recv)->recv_qp = recv_qp;
  (*recv)->recv_sgl = recv_sgl;
  (*recv)->buf = buf;
}


// Set up a struct that stores pending writes
void set_up_pending_writes(struct pending_writes **p_writes, uint32_t size)
{
  int i;
  (*p_writes) = (struct pending_writes*) malloc(sizeof(struct pending_writes));
  memset((*p_writes), 0, sizeof(struct pending_writes));
  //(*p_writes)->write_ops = (struct write_op*) malloc(size * sizeof(struct write_op));
  (*p_writes)->g_id = (uint64_t*) malloc(size * sizeof(uint64_t));
  (*p_writes)->w_state = (enum write_state*) malloc(size * sizeof(enum write_state));
  (*p_writes)->session_id = (uint32_t*) malloc(size * sizeof(uint32_t));
  (*p_writes)->acks_seen = (uint8_t*) malloc(size * sizeof(uint8_t));
  (*p_writes)->flr_id = (uint8_t*) malloc(size * sizeof(uint8_t));
  (*p_writes)->is_local = (bool*) malloc(size * sizeof(bool));
  (*p_writes)->session_has_pending_write = (bool*) malloc(SESSIONS_PER_THREAD * sizeof(bool));
  (*p_writes)->ptrs_to_ops = (struct prepare**) malloc(size * sizeof(struct prepare*));

  memset((*p_writes)->g_id, 0, size * sizeof(uint64_t));
  (*p_writes)->prep_fifo = (struct prep_fifo *) malloc(sizeof(struct prep_fifo));
  memset((*p_writes)->prep_fifo, 0, sizeof(struct prep_fifo));
  (*p_writes)->prep_fifo->prep_message =
    (struct prep_message*) malloc(PREP_FIFO_SIZE * sizeof(struct prep_message));
  memset((*p_writes)->prep_fifo->prep_message, 0, PREP_FIFO_SIZE * sizeof(struct prep_message));
  //init_fifo(&(*p_writes)->prep_fifo, PREP_FIFO_SIZE * sizeof(struct prep_message));
  assert((*p_writes)->prep_fifo != NULL);
  //  memset((*p_writes)->write_ops, 0, size * sizeof(struct write_op));
  //  memset((*p_writes)->unordered_writes, 0, size * sizeof(uint32_t));
  memset((*p_writes)->acks_seen, 0, size * sizeof(uint8_t));
  for (i = 0; i < SESSIONS_PER_THREAD; i++) (*p_writes)->session_has_pending_write[i] = false;
  for (i = 0; i < size; i++) {
    (*p_writes)->w_state[i] = INVALID;
  }
  struct prep_message *preps = (*p_writes)->prep_fifo->prep_message;
  for (i = 0; i < PREP_FIFO_SIZE; i++) {
      preps[i].opcode = CACHE_OP_PUT;
      for(uint16_t j = 0; j < MAX_PREP_COALESCE; j++) {
          preps[i].prepare[j].opcode = CACHE_OP_PUT;
          preps[i].prepare[j].val_len = HERD_VALUE_SIZE >> SHIFT_BITS;
      }
  }
}


// Set up a struct that stores pending writes
void set_up_completed_writes(struct completed_writes **c_writes, uint32_t size)
{
  int i;
  (*c_writes)->w_ops = (struct write_op**) malloc(size * sizeof(struct write_op*));
  (*c_writes)->w_state = (enum write_state*) malloc(size * sizeof(enum write_state));
  (*c_writes)->p_writes_ptr = (uint32_t*) malloc(size * sizeof(uint32_t));
  memset((*c_writes)->w_ops, 0, size * sizeof(struct write_op));
  memset((*c_writes)->p_writes_ptr, 0, size * sizeof(uint32_t));
  (*c_writes)->push_ptr = 0;
  (*c_writes)->pull_ptr = 0;
  for (i = 0; i < size; i++) {
    (*c_writes)->w_state[i] = INVALID;
  }
}


// set the different queue depths for client's queue pairs
void set_up_queue_depths_ldr_flr(int** recv_q_depths, int** send_q_depths, int protocol)
{
  /* -------LEADER-------------
  * 1st Dgram send Prepares -- receive ACKs
  * 2nd Dgram send Commits  -- receive Writes
  * 3rd Dgram send Credits  -- receive Credits
  *
    * ------FOLLOWER-----------
  * 1st Dgram receive prepares -- send Acks
  * 2nd Dgram receive Commits  -- send Writes
  * 3rd Dgram receive Credits  -- send Credits
  * */
  if (protocol == FOLLOWER) {
    *send_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
    *recv_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
    (*recv_q_depths)[PREP_ACK_QP_ID] = FLR_RECV_PREP_Q_DEPTH;
    (*recv_q_depths)[COMMIT_W_QP_ID] = FLR_RECV_COM_Q_DEPTH;
    (*recv_q_depths)[FC_QP_ID] = FLR_RECV_CR_Q_DEPTH;
    (*send_q_depths)[PREP_ACK_QP_ID] = FLR_SEND_ACK_Q_DEPTH;
    (*send_q_depths)[COMMIT_W_QP_ID] = FLR_SEND_W_Q_DEPTH;
    (*send_q_depths)[FC_QP_ID] = FLR_SEND_CR_Q_DEPTH;
  }
  else if (protocol == LEADER) {
    *send_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
    *recv_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
    (*recv_q_depths)[PREP_ACK_QP_ID] = LDR_RECV_ACK_Q_DEPTH;
    (*recv_q_depths)[COMMIT_W_QP_ID] = LDR_RECV_W_Q_DEPTH;
    (*recv_q_depths)[FC_QP_ID] = LDR_RECV_CR_Q_DEPTH;
    (*send_q_depths)[PREP_ACK_QP_ID] = LDR_SEND_PREP_Q_DEPTH;
    (*send_q_depths)[COMMIT_W_QP_ID] = LDR_SEND_COM_Q_DEPTH;
    (*send_q_depths)[FC_QP_ID] = LDR_SEND_CR_Q_DEPTH;
  }
  else check_protocol(protocol);
}

// Prepost Receives on the Leader Side
// Post receives for the coherence traffic in the init phase
void pre_post_recvs(struct hrd_ctrl_blk *cb, uint32_t* push_ptr, bool enable_mcast_receive, struct mcast_essentials *mcast, void* buf,
                    uint32_t max_reqs, uint32_t number_of_recvs, uint16_t QP_ID, uint32_t message_size)
{
  uint32_t i;//, j;
  for(i = 0; i < number_of_recvs; i++) {
      if (enable_mcast_receive) {
        hrd_post_dgram_recv(mcast->recv_qp,	(buf + *push_ptr * message_size),
                            message_size, mcast->recv_mr->lkey);
      }
      else hrd_post_dgram_recv(cb->dgram_qp[QP_ID],
                               (buf + *push_ptr * message_size), message_size, cb->dgram_buf_mr->lkey);
      MOD_ADD(*push_ptr, max_reqs);
  }
}


// set up some basic leader buffers
void set_up_ldr_ops(struct cache_op **ops, struct mica_resp **resp,
                    struct mica_resp **commit_resp,
                    struct mica_op **buf, struct commit_fifo **com_fifo)
{
  int i;

  uint16_t cache_op_size = sizeof(struct cache_op);
  uint16_t mica_resp_size = sizeof(struct mica_resp);
  *buf = memalign(4096, COH_BUF_SIZE);
  *ops = memalign(4096, CACHE_BATCH_SIZE *  cache_op_size);
  *com_fifo =  malloc(sizeof(struct commit_fifo));
  (*com_fifo)->commits = (struct com_message *) malloc(COMMIT_FIFO_SIZE * sizeof(struct com_message));
  *resp = memalign(4096, CACHE_BATCH_SIZE * mica_resp_size);
  *commit_resp = memalign(4096, LEADER_PENDING_WRITES * mica_resp_size);
  memset((*com_fifo)->commits, 0, COMMIT_FIFO_SIZE * sizeof(struct com_message));
  (*com_fifo)->push_ptr = 0;
  (*com_fifo)->pull_ptr = 0; (*com_fifo)->size = 0;
  for(i = 0; i <  CACHE_BATCH_SIZE; i++) (*resp)[i].type = EMPTY;
  for(i = 0; i <  LEADER_PENDING_WRITES; i++) (*commit_resp)[i].type = EMPTY;
  for(i = 0; i <  COMMIT_CREDITS; i++) {
      (*com_fifo)->commits[i].opcode = CACHE_OP_PUT;
  }
  assert(*ops != NULL && *resp != NULL && *commit_resp != NULL && *buf != NULL);

}

// Set up the memory registrations required in the leader if there is no Inlining
void set_up_ldr_mrs(struct ibv_mr **prep_mr, void *prep_buf,
                    struct ibv_mr **com_mr, void *com_buf,
                    struct hrd_ctrl_blk *cb)
{
  if (!LEADER_PREPARE_ENABLE_INLINING) {
   *prep_mr = register_buffer(cb->pd, (void*)prep_buf, PREP_FIFO_SIZE * sizeof(struct prep_message));
  }
  if (!COM_ENABLE_INLINING) *com_mr = register_buffer(cb->pd, com_buf,
                                                      COMMIT_CREDITS * sizeof(struct com_message));
}

// Set up all leader WRs
void set_up_ldr_WRs(struct ibv_send_wr *prep_send_wr, struct ibv_sge *prep_send_sgl,
                    struct ibv_recv_wr *ack_recv_wr, struct ibv_sge *ack_recv_sgl,
                    struct ibv_send_wr *com_send_wr, struct ibv_sge *com_send_sgl,
                    struct mica_op *buf, uint16_t t_id, uint16_t remote_thread,
                    struct hrd_ctrl_blk *cb, struct ibv_mr *prep_mr, struct ibv_mr *com_mr,
                    struct mcast_essentials *mcast)
{
  uint16_t i, j;
  //BROADCAST WRs and credit Receives
  for (j = 0; j < MAX_BCAST_BATCH; j++) { // Number of Broadcasts
    //prep_send_sgl[j].addr = (uint64_t) (uintptr_t) (buf + j);
    if (LEADER_PREPARE_ENABLE_INLINING == 0) prep_send_sgl[j].lkey = prep_mr->lkey;
    if (!COM_ENABLE_INLINING) com_send_sgl[j].lkey = com_mr->lkey;
    for (i = 0; i < MESSAGES_IN_BCAST; i++) {
      uint16_t rm_id = i;
      uint16_t index = (j * MESSAGES_IN_BCAST) + i;
      assert (index < MESSAGES_IN_BCAST_BATCH);
      if (ENABLE_MULTICAST == 1) {
        prep_send_wr[index].wr.ud.ah = mcast->send_ah;
        prep_send_wr[index].wr.ud.remote_qpn = mcast->qpn;
        prep_send_wr[index].wr.ud.remote_qkey = mcast->qkey;
      }
      else {
        prep_send_wr[index].wr.ud.ah = remote_follower_qp[rm_id][remote_thread][PREP_ACK_QP_ID].ah;
        prep_send_wr[index].wr.ud.remote_qpn = (uint32) remote_follower_qp[rm_id][remote_thread][PREP_ACK_QP_ID].qpn;
        prep_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        com_send_wr[index].wr.ud.ah = remote_follower_qp[rm_id][remote_thread][COMMIT_W_QP_ID].ah;
        com_send_wr[index].wr.ud.remote_qpn = (uint32) remote_follower_qp[rm_id][remote_thread][COMMIT_W_QP_ID].qpn;
        com_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
      }
      prep_send_wr[index].opcode = IBV_WR_SEND;
      prep_send_wr[index].num_sge = 1;
      prep_send_wr[index].sg_list = &prep_send_sgl[j];
      com_send_wr[index].opcode = IBV_WR_SEND;
      com_send_wr[index].num_sge = 1;
      com_send_wr[index].sg_list = &com_send_sgl[j];
      if (LEADER_PREPARE_ENABLE_INLINING == 1) prep_send_wr[index].send_flags = IBV_SEND_INLINE;
      else prep_send_wr[index].send_flags = 0;
      if (COM_ENABLE_INLINING == 1) com_send_wr[index].send_flags = IBV_SEND_INLINE;
      prep_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &prep_send_wr[index + 1];
      com_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &com_send_wr[index + 1];
    }
  }

  // ACK Receives
  for (i = 0; i < LDR_MAX_RECV_ACK_WRS; i++) {
    ack_recv_sgl[i].length = LDR_ACK_RECV_SIZE;
    if (ENABLE_MULTICAST == 1)
      ack_recv_sgl[i].lkey = mcast->recv_mr->lkey;
    else  ack_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    ack_recv_wr[i].sg_list = &ack_recv_sgl[i];
    ack_recv_wr[i].num_sge = 1;
  }
}

// The Leader sends credits to the followers when it receives their writes
// The follower sends credits to the leader when it receives commit messages
void set_up_credits_and_WRs(uint16_t credits[][FOLLOWER_MACHINE_NUM], struct ibv_send_wr* credit_send_wr,
                        struct ibv_sge* credit_send_sgl, struct ibv_recv_wr* credit_recv_wr,
                        struct ibv_sge* credit_recv_sgl, struct hrd_ctrl_blk *cb, int protocol,
                        uint32_t max_credit_wrs, uint32_t max_credit_recvs)
{
  int i = 0;
//  int max_credt_wrs = protocol == FOLLOWER ?  SC_MAX_CREDIT_WRS : MAX_CREDIT_WRS;
//  int max_credit_recvs = protocol == FOLLOWER ? SC_MAX_CREDIT_RECVS : MAX_CREDIT_RECVS;
  // Credits
  if (protocol == FOLLOWER)
    ;//for (i = 0; i < MACHINE_NUM; i++) credits[SC_UPD_VC][i] = SC_CREDITS;
  else {
    for (i = 0; i < FOLLOWER_MACHINE_NUM; i++) {
      credits[PREP_VC][i] = PREPARE_CREDITS;
      credits[COMM_VC][i] = COMMIT_CREDITS;
    }
  }
  // Credit WRs
  for (i = 0; i < max_credit_wrs; i++) {
    credit_send_sgl->length = 0;
    credit_send_wr[i].opcode = IBV_WR_SEND; // No immediate is required for the credits
    credit_send_wr[i].num_sge = 0;
    credit_send_wr[i].sg_list = credit_send_sgl;
    credit_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    credit_send_wr[i].next = NULL;
    credit_send_wr[i].send_flags = IBV_SEND_INLINE;
  }
  //Credit Receives
  credit_recv_sgl->length = 64;
  credit_recv_sgl->lkey = cb->dgram_buf_mr->lkey;
  credit_recv_sgl->addr = (uintptr_t) &cb->dgram_buf[0];
  for (i = 0; i < max_credit_recvs; i++) {
    credit_recv_wr[i].sg_list = credit_recv_sgl;
    credit_recv_wr[i].num_sge = 1;
  }
}

/* ---------------------------------------------------------------------------
------------------------------FOLLOWER --------------------------------------
---------------------------------------------------------------------------*/

// Set up all Follower WRs
void set_up_follower_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                         struct ibv_recv_wr *prep_recv_wr, struct ibv_sge *prep_recv_sgl,
                         struct ibv_send_wr *w_send_wr, struct ibv_sge *w_send_sgl,
                         struct ibv_recv_wr *com_recv_wr, struct ibv_sge *com_recv_sgl,
                         uint16_t remote_thread,
                         struct hrd_ctrl_blk *cb, struct ibv_mr *w_mr,
                         struct mcast_essentials *mcast)
{
  uint16_t i, j;
    // ACKS
    ack_send_wr->wr.ud.ah = remote_leader_qp[remote_thread][PREP_ACK_QP_ID].ah;
    ack_send_wr->wr.ud.remote_qpn = (uint32) remote_leader_qp[remote_thread][PREP_ACK_QP_ID].qpn;
    ack_send_wr->wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    ack_send_wr->opcode = IBV_WR_SEND;
    ack_send_wr->send_flags = IBV_SEND_INLINE;
    ack_send_sgl->length = FLR_ACK_SEND_SIZE;
    ack_send_wr->num_sge = 1;
    ack_send_wr->sg_list = ack_send_sgl;
    ack_send_wr->next = NULL;
    // WRITES
    for (i = 0; i < FLR_MAX_W_WRS; ++i) {
        w_send_wr[i].wr.ud.ah = remote_leader_qp[remote_thread][COMMIT_W_QP_ID].ah;
        w_send_wr[i].wr.ud.remote_qpn = (uint32) remote_leader_qp[remote_thread][COMMIT_W_QP_ID].qpn;
        if (FLR_PREPARE_ENABLE_INLINING) w_send_wr[i].send_flags = IBV_SEND_INLINE;
        else {
            w_send_sgl[i].lkey = w_mr->lkey;
            w_send_wr[i].send_flags = 0;
        }
        w_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        w_send_wr[i].opcode = IBV_WR_SEND;
        w_send_wr[i].num_sge = 1;
        w_send_wr[i].sg_list = &w_send_sgl[i];
    }
    // PREP RECVs
    for (i = 0; i < FLR_MAX_RECV_PREP_WRS; i++) {
        prep_recv_sgl[i].length = (uint32_t)FLR_PREP_RECV_SIZE;
        if (ENABLE_MULTICAST == 1)
            prep_recv_sgl[i].lkey = mcast->recv_mr->lkey;
        else  prep_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        prep_recv_wr[i].sg_list = &prep_recv_sgl[i];
        prep_recv_wr[i].num_sge = 1;
    }
    // COM RECVs
    for (i = 0; i < FLR_MAX_RECV_COM_WRS; i++) {
        com_recv_sgl[i].length = (uint32_t)FLR_COM_RECV_SIZE;
        if (ENABLE_MULTICAST == 1)
            com_recv_sgl[i].lkey = mcast->recv_mr->lkey;
        else  com_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        com_recv_wr[i].sg_list = &com_recv_sgl[i];
        com_recv_wr[i].num_sge = 1;
    }

}


/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
void check_protocol(int protocol)
{
    if (protocol != FOLLOWER && protocol != LEADER) {
        red_printf("Wrong protocol specified when setting up the queue depths %d \n", protocol);
        assert(false);
    }
}

/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/
// wrapper around getaddrinfo socket function
int get_addr(char *dst, struct sockaddr *addr)
{
    struct addrinfo *res;
    int ret;
    ret = getaddrinfo(dst, NULL, NULL, &res);
    if (ret) {
        printf("getaddrinfo failed - invalid hostname or IP address %s\n", dst);
        return ret;
    }
    memcpy(addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return ret;
}

//Handle the addresses
void resolve_addresses(struct mcast_info *mcast_data)
{
    int ret, i, clt_id = mcast_data->clt_id;
    char mcast_addr[40];
    // Source addresses (i.e. local IPs)
    mcast_data->src_addr = (struct sockaddr*)&mcast_data->src_in;
    ret = get_addr(local_IP, ((struct sockaddr *)&mcast_data->src_in)); // to bind
    if (ret) printf("Client: failed to get src address \n");
    for (i = 0; i < MCAST_QPS; i++) {
        ret = rdma_bind_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr);
        if (ret) perror("Client: address bind failed");
    }
    // Destination addresses(i.e. multicast addresses)
    for (i = 0; i < MCAST_GROUPS_PER_CLIENT; i ++) {
        mcast_data->dst_addr[i] = (struct sockaddr*)&mcast_data->dst_in[i];
        int m_cast_group_id = clt_id * MACHINE_NUM + i;
        sprintf(mcast_addr, "224.0.%d.%d", m_cast_group_id / 256, m_cast_group_id % 256);
        // printf("mcast addr %d: %s\n", i, mcast_addr);
        ret = get_addr((char*) &mcast_addr, ((struct sockaddr *)&mcast_data->dst_in[i]));
        if (ret) printf("Client: failed to get dst address \n");
    }
}

// Set up the Send and Receive Qps for the multicast
void set_up_qp(struct cm_qps* qps, int max_recv_q_depth)
{
    int ret, i, recv_q_depth;
    // qps[0].pd = ibv_alloc_pd(qps[0].cma_id->verbs); //new
    for (i = 0; i < MCAST_QPS; i++) {
        qps[i].pd = ibv_alloc_pd(qps[i].cma_id->verbs);
        if (i > 0) qps[i].pd = qps[0].pd;
        recv_q_depth = i == RECV_MCAST_QP ? max_recv_q_depth : 1; // TODO fix this
        qps[i].cq = ibv_create_cq(qps[i].cma_id->verbs, recv_q_depth, &qps[i], NULL, 0);
        struct ibv_qp_init_attr init_qp_attr;
        memset(&init_qp_attr, 0, sizeof init_qp_attr);
        init_qp_attr.cap.max_send_wr = 1;
        init_qp_attr.cap.max_recv_wr = (uint32) recv_q_depth;
        init_qp_attr.cap.max_send_sge = 1;
        init_qp_attr.cap.max_recv_sge = 1;
        init_qp_attr.qp_context = &qps[i];
        init_qp_attr.sq_sig_all = 0;
        init_qp_attr.qp_type = IBV_QPT_UD;
        init_qp_attr.send_cq = qps[i].cq;
        init_qp_attr.recv_cq = qps[i].cq;
        ret = rdma_create_qp(qps[i].cma_id, qps[i].pd, &init_qp_attr);
        if (ret) printf("unable to create QP \n");
    }
}

// Initial function to call to setup multicast, this calls the rest of the relevant functions
void setup_multicast(struct mcast_info *mcast_data, int recv_q_depth)
{
    int ret, i, clt_id = mcast_data->clt_id;
    static enum rdma_port_space port_space = RDMA_PS_UDP;
    // Create the channel
    mcast_data->channel = rdma_create_event_channel();
    if (!mcast_data->channel) {
        printf("Client %d :failed to create event channel\n", mcast_data->clt_id);
        exit(1);
    }
    // Set up the cma_ids
    for (i = 0; i < MCAST_QPS; i++ ) {
        ret = rdma_create_id(mcast_data->channel, &mcast_data->cm_qp[i].cma_id, &mcast_data->cm_qp[i], port_space);
        if (ret) printf("Client %d :failed to create cma_id\n", mcast_data->clt_id);
    }
    // deal with the addresses
    resolve_addresses(mcast_data);
    // set up the 2 qps
    set_up_qp(mcast_data->cm_qp, recv_q_depth);

    struct rdma_cm_event* event;
    for (i = 0; i < MCAST_GROUPS_PER_CLIENT; i ++) {
        int qp_i = i == machine_id ? SEND_MCAST_QP : RECV_MCAST_QP;
        ret = rdma_resolve_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr, mcast_data->dst_addr[i], 20000);
        if (ret) printf("Client %d: failed to resolve address: %d, qp_i %d \n", clt_id, i, qp_i);
        if (ret) perror("Reason");
        while (rdma_get_cm_event(mcast_data->channel, &event) == 0) {
            switch (event->event) {
                case RDMA_CM_EVENT_ADDR_RESOLVED:
                    // printf("Client %d: RDMA ADDRESS RESOLVED address: %d \n", clt_id, i);
                    ret = rdma_join_multicast(mcast_data->cm_qp[qp_i].cma_id, mcast_data->dst_addr[i], mcast_data);
                    if (ret) printf("unable to join multicast \n");
                    break;
                case RDMA_CM_EVENT_MULTICAST_JOIN:
                    if (i == machine_id) mcast_data->mcast_ud_param = event->param.ud;
                    // printf("RDMA JOIN MUlTICAST EVENT %d \n", i);
                    break;
                case RDMA_CM_EVENT_MULTICAST_ERROR:
                default:
                    break;
            }
            rdma_ack_cm_event(event);
            if (event->event == RDMA_CM_EVENT_MULTICAST_JOIN) break;
        }
        if (i != RECV_MCAST_QP) {
            // destroying the QPs works fine but hurts performance...
            //  rdma_destroy_qp(mcast_data->cm_qp[i].cma_id);
            //  rdma_destroy_id(mcast_data->cm_qp[i].cma_id);
        }
    }
    // rdma_destroy_event_channel(mcast_data->channel);
    // if (mcast_data->mcast_ud_param == NULL) mcast_data->mcast_ud_param = event->param.ud;
}


// call to test the multicast
void multicast_testing(struct mcast_essentials *mcast, int clt_gid, struct hrd_ctrl_blk *cb)
{

    struct ibv_wc mcast_wc;
    printf ("Client: Multicast Qkey %u and qpn %u \n", mcast->qkey, mcast->qpn);


    struct ibv_sge mcast_sg;
    struct ibv_send_wr mcast_wr;
    struct ibv_send_wr *mcast_bad_wr;

    memset(&mcast_sg, 0, sizeof(mcast_sg));
    mcast_sg.addr	  = (uintptr_t)cb->dgram_buf;
    mcast_sg.length = 10;
    //mcast_sg.lkey	  = cb->dgram_buf_mr->lkey;

    memset(&mcast_wr, 0, sizeof(mcast_wr));
    mcast_wr.wr_id      = 0;
    mcast_wr.sg_list    = &mcast_sg;
    mcast_wr.num_sge    = 1;
    mcast_wr.opcode     = IBV_WR_SEND_WITH_IMM;
    mcast_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    mcast_wr.imm_data   = (uint32) clt_gid + 120 + (machine_id * 10);
    mcast_wr.next       = NULL;

    mcast_wr.wr.ud.ah          = mcast->send_ah;
    mcast_wr.wr.ud.remote_qpn  = mcast->qpn;
    mcast_wr.wr.ud.remote_qkey = mcast->qkey;

    if (ibv_post_send(cb->dgram_qp[0], &mcast_wr, &mcast_bad_wr)) {
        fprintf(stderr, "Error, ibv_post_send() failed\n");
        assert(false);
    }

    printf("THe mcast was sent, I am waiting for confirmation imm data %d\n", mcast_wr.imm_data);
    hrd_poll_cq(cb->dgram_send_cq[0], 1, &mcast_wc);
    printf("The mcast was sent \n");
    hrd_poll_cq(mcast->recv_cq, 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
    hrd_poll_cq(mcast->recv_cq, 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
    hrd_poll_cq(mcast->recv_cq, 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);

    exit(0);
}
