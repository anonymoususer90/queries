#ifndef STAT_PAGE_READ_IO_BREAKDOWN_H
#define STAT_PAGE_READ_IO_BREAKDOWN_H

#include <unistd.h>

#ifndef PANDORA_MAX_LEVEL
#define PANDORA_MAX_LEVEL (32)
#endif /* PANDORA_MAX_LEVEL */

extern bool stat_is_valid_level[PANDORA_MAX_LEVEL];
extern uint64_t stat_heap_io_uring[PANDORA_MAX_LEVEL];
extern uint64_t stat_heap_shared_buffer_io[PANDORA_MAX_LEVEL];
extern uint64_t stat_heap_shared_buffer_hit[PANDORA_MAX_LEVEL];
extern uint64_t stat_idx_shared_buffer_io;
extern uint64_t stat_idx_shared_buffer_hit;
extern uint64_t stat_ebi_io;
extern uint64_t stat_ebi_hit;

extern bool isOLAP;

#endif /* STAT_PAGE_READ_IO_BREAKDOWN_H */

