#ifndef STAT_TPCC_H
#define STAT_TPCC_H

#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "c.h"

/* Return elapsed time as nanosecond */
static inline uint64_t
stat_tpcc_get_time_diff(struct timespec start, struct timespec end)
{
	uint64_t start_time = start.tv_sec * 1000000000 + start.tv_nsec;
	uint64_t end_time = end.tv_sec * 1000000000 + end.tv_nsec;
	Assert(end_time >= start_time);
	return end_time - start_time;
}

/* INSERT */
extern volatile uint64_t stat_tpcc_insert_cnt;
extern volatile uint64_t stat_tpcc_insert_total_time;
extern volatile uint64_t stat_tpcc_insert_max_time;
extern volatile uint64_t stat_tpcc_insert_min_time;

/* UPDATE */
extern volatile uint64_t stat_tpcc_update_cnt;
extern volatile uint64_t stat_tpcc_update_total_time;
extern volatile uint64_t stat_tpcc_update_max_time;
extern volatile uint64_t stat_tpcc_update_min_time;

/* BREAKDOWN UPDATE */
extern volatile bool stat_tpcc_breakdown_update_exec_update_flag;
extern volatile uint64_t stat_tpcc_breakdown_update_cnt;
extern volatile uint64_t stat_tpcc_breakdown_update_total;
extern volatile uint64_t stat_tpcc_breakdown_update_ExecUpdateAct;

/* BREAKDOWN INSERT */
extern volatile bool stat_tpcc_breakdown_insert_exec_insert_flag;
extern volatile uint64_t stat_tpcc_breakdown_insert_cnt;
extern volatile uint64_t stat_tpcc_breakdown_insert_total;
extern volatile uint64_t stat_tpcc_breakdown_insert_tuple_insert;
extern volatile uint64_t stat_tpcc_breakdown_insert_read_buffer_before;
extern volatile uint64_t stat_tpcc_breakdown_insert_read_buffer;
extern volatile uint64_t stat_tpcc_breakdown_insert_read_buffer_after;
extern volatile uint64_t stat_tpcc_breakdown_insert_buffer_lock;
extern volatile uint64_t stat_tpcc_breakdown_insert_record_write;
extern volatile uint64_t stat_tpcc_breakdown_insert_LOG;
extern volatile uint64_t stat_tpcc_breakdown_insert_pandora_meta_mem_lock;
extern volatile uint64_t stat_tpcc_breakdown_insert_pandora_mempart_lock;
extern volatile uint64_t stat_tpcc_breakdown_insert_ExecInsertIndexTuples;
extern volatile uint64_t stat_tpcc_breakdown_insert_HapGetPartIdHash;
extern volatile uint64_t stat_tpcc_breakdown_insert_fkey_check;
extern volatile uint64_t stat_tpcc_breakdown_insert_build_hidden;
extern volatile uint64_t stat_tpcc_breakdown_insert_deconstruct_hidden;
extern volatile uint64_t stat_tpcc_breakdown_insert_index_insert;
extern volatile uint64_t stat_tpcc_breakdown_insert_heap_insert;
extern volatile uint64_t
stat_tpcc_breakdown_insert_heap_insert_before_get_buffer;
extern volatile uint64_t
stat_tpcc_breakdown_insert_heap_insert_get_buffer;
extern volatile uint64_t
stat_tpcc_breakdown_insert_heap_insert_after_get_buffer;

#endif /* STAT_TPCC_H */
