/*-------------------------------------------------------------------------
 *
 * pandora_mempart_buf.h
 *	  PANDORA's memaprt buffer.
 *
 *
 * src/include/pandora/pandora_mempart_buf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PANDORA_MEMPART_BUF_H
#define PANDORA_MEMPART_BUF_H

#include "storage/lwlock.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "pandora/pandora_md.h"

#define PANDORA_MEMPART_COUNT (64)

/* 1024 means 8MB (Page size 8KB * 1024) */
#define PANDORA_MEMPART_PAGE_COUNT (1024 * 2) 

#define PANDORA_MEMPART_BUFFER_TOTAL_PAGE_COUNT \
	(PANDORA_MEMPART_COUNT * PANDORA_MEMPART_PAGE_COUNT)

extern int PandoraGetMempartIdFromBufId(int buf_id);

extern LWLock *PandoraGetMempartLock(int mempart_id);

extern int PandoraGetMempartBufId(int buf_id);

extern bool PandoraCheckMempartBuf(int buf_id);

extern BlockNumber PandoraGetMempartBlockCount(RelFileNode node,
											   PandoraPartNumber partNum);

#endif /* PANDORA_MEMPART_BUF_H */
