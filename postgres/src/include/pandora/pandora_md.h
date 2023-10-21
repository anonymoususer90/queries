/*-------------------------------------------------------------------------
 *
 * pandora_md.h
 *    Pandora Magnetic Disk (used in traditional buffer pool)
 *
 *
 * src/include/pandora/pandora_md.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PANDORA_MD_H
#define PANDORA_MD_H

#include <liburing.h>

#include "pg_config.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "pandora/pandora.h"

#define PandoraLseekBlocks(fd) \
	((BlockNumber) (lseek(fd, 0, SEEK_END) / BLCKSZ))

extern BlockNumber
PandoraNblocks(RelFileNode node, PandoraPartLevel partLvl,
			   PandoraPartNumber partNum, PandoraPartGenNo partGen);
extern void
PandoraCreatePartitionFile(RelFileNode node, PandoraPartLevel partLvl,
			  PandoraPartNumber partNum, PandoraPartGenNo partGen);
extern void
PandoraExtend(RelFileNode node, PandoraPartLevel partLvl,
			  PandoraPartNumber partNum, PandoraPartGenNo partGen,
			  BlockNumber blockNum, char *buffer);
extern bool
PandoraWrite(RelFileNode node, PandoraPartLevel partLvl,
			 PandoraPartNumber partNum, PandoraPartGenNo partGen,
			 BlockNumber blockNum, char *buffer);
extern bool
PandoraWriteBatch(RelFileNode node, PandoraPartLevel partLvl,
			 PandoraPartNumber partNum, PandoraPartGenNo partGen,
			 uint64_t offset, struct iovec *iovs, int64_t iovcnt);
extern void
PandoraWriteback(RelFileNode node, PandoraPartLevel partLvl,
				 PandoraPartNumber partNum, PandoraPartGenNo partGen,
				 BlockNumber blockNum, BlockNumber nblocks);
extern void
PandoraRead(RelFileNode node, PandoraPartLevel partLvl,
			PandoraPartNumber partNum, PandoraPartGenNo partGen,
			BlockNumber blockNum, char *buffer);
extern int
PandoraRequest(struct io_uring* ring, int fd, struct iovec *iovs, 
			   BlockNumber blockNum, int req_page_num, void *io_info);

extern void
PandoraMempartExtend(RelFileNode node, PandoraPartLevel partLvl,
			  PandoraPartNumber partNum, PandoraPartGenNo partGen,
			  BlockNumber blockNum, char *buffer);

extern bool
PandoraMempartWrite(RelFileNode node, PandoraPartLevel partLvl,
			 PandoraPartNumber partNum, PandoraPartGenNo partGen,
			 char *buffer, int nblock);

extern void
PandoraMempartRead(RelFileNode node, PandoraPartLevel partLvl,
				   PandoraPartNumber partNum, PandoraPartGenNo partGen,
				   char *buffer);
#endif /* PANDORA_MD_H */
