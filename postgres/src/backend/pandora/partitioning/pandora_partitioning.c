/*
 * pandora_partitioning.c
 *
 * Pandora Paritioning Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/pandora_partitioning.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef PANDORA

#include "postgres.h"

#include <math.h>
#include <unistd.h>
#include "utils/dsa.h"
#include "utils/rel.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/itemptr.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "miscadmin.h"

#include "pg_refcnt.h"
#include "pandora/pandora.h"
#include "pandora/pandora_relation_meta.h"
#include "pandora/pandora_partitioning.h"
#include "pandora/pandora_mempart_buf.h"
#include "pandora/pandora_md.h"

/* src/include/postmaster/partition_mgr.h */
dsa_area* pandoraPartitionDsaArea;

dsa_pointer
PandoraPartitioningInit(dsa_area* area)
{
	elog(LOG, "PandoraInit");
	/*
	dsa_pointer dsa_ebitree, dsa_sentinel;

	dsa_ebitree =
		dsa_allocate_extended(area, sizeof(EbiTreeData), DSA_ALLOC_ZERO);
	ebitree = EbiConvertToTree(area, dsa_ebitree);

	dsa_sentinel = EbiCreateNode(area);

	ebitree->root = da_sentinel;
	ebitree->recent_node = dsa_sentinel;

	return dsa_ebitree;
	*/
	return (dsa_pointer) NULL;
}

/*
 * PandoraBeginRepartitioning
 *
 * When starting repartitioning, this function is called by repartitioning 
 * worker. In here, we do allocate and set values for repartitioning.
 */
static void
PandoraBeginRepartitioning(PandoraRepartitioningDesc *desc,	Oid relationOid,
						   PandoraPartLevel partitionLevel,
						   PandoraPartNumber minPartitionNumber,
						   PandoraPartNumber maxPartitionNumber,
						   PandoraPartLevel targetPartitionLevel,
						   bool isPartitioningWorker)
{
	PandoraPartitionScanDesc *pscan = &(desc->upperScan);
	PandoraPartitionInsertDesc *insertDesc = &(desc->lowerInsert);
	PandoraRelationMeta *meta = desc->meta;
	MemoryContext oldContext;

	Assert(partitionLevel < targetPartitionLevel);

	/*
	 * Normal partitioning worker can open the relation, but the checkpointer
	 * can't. So if this repartitioning is being executed by checkpointer, use
	 * backed up data rather than opening the relation.
	 */
	if (likely(isPartitioningWorker))
		desc->relation = table_open(relationOid, AccessShareLock);
	else
		desc->relation = &(meta->relation);

	/* Init partition scan domain. */
	pscan->meta = meta;
	pscan->relation = desc->relation;
	pscan->partitionLevel = partitionLevel;
	pscan->minPartitionNumber = minPartitionNumber;
	pscan->maxPartitionNumber = maxPartitionNumber;

	/* Init partition scan. */
	pscan->currentPartitionNumber = minPartitionNumber;
	pscan->currentPartitionGenerationNumber = 
						PandoraGetPartGenerationNumber(meta, 
											pscan->partitionLevel,
											pscan->currentPartitionNumber);

	/*
	 * There are two types of objects that repartition: partitioning worker and
	 * checkpointer.
	 * 
	 * Normally, only the partitioning worker does the repartitioning, but when
	 * the PostgreSQL server shuts down, the checkpointer does the last
	 * repartitioning, and it only repartitions level 0, including the newest
	 * mempartition.
	 * 
	 * By default, a mempartition at level 0 is not evicted unless the threshold
	 * is fully filled, so the number of pages in a tiered mempartition has a
	 * constant value.
	 * Therefore, in a normal case, when the partitioning worker repartitions
	 * level 0, only tiered mempartitions are repartitioned, so the constant
	 * value mentioned above can be used.
	 * 
	 * However, the repartitioning of level 0 executed by the checkpointer when
	 * the server is shut down will repartition including the newest
	 * mempartition that has not fully filled the threshold.
	 * Therefore, in this case, the page count of the newest mempartition does
	 * not have a constant value, so the page count must be calculated using the
	 * record count recorded in the relation metadata.
	 */
	if (likely(isPartitioningWorker))
	{
		/* Normal case: partitioning worker */
		pscan->nblocks = partitionLevel == 0 ?
			PandoraMempartPageCount :
			NBlocksUsingRecordCount(meta->realRecordsCount[partitionLevel][minPartitionNumber],
									meta->recordNumsPerBlock);
	}
	else
	{
		/* PostgreSQL server is shutting down: checkpointer */
		Assert(partitionLevel == 0);

		pscan->nblocks = (minPartitionNumber == meta->partitionNumberLevelZero[1]) ?
			NBlocksUsingRecordCount(meta->recordCountsLevelZero[0],
									meta->recordNumsPerBlock) :
			PandoraMempartPageCount;
	}

	pscan->startBlock = 0;			/* We begin with the first page. */
	pscan->firstScan = true;

	pscan->currentBuffer = InvalidBuffer;
	pscan->currentBlock = InvalidBlockNumber;
	pscan->currentTuple = NULL;
	pscan->pinOtherBufferNums = 0;

	pscan->isPartitioningWorker = isPartitioningWorker;

#ifdef PANDORA_DEBUG
	pscan->repartitionedNtuple_partition = 0;
#endif /* PANDORA_DEBUG */

	desc->deltaNums = 0;
	desc->parentBlocks = pscan->nblocks;
	desc->childBlocks = 0;

#ifdef PANDORA_DEBUG
	fprintf(stderr, "[PANDORA] PandoraBeginRepartitioning, level: %u, number: %u, nblocks: %u\n",
					partitionLevel, minPartitionNumber, pscan->nblocks);
#endif /* PANDORA DEBUG */

	/* Init insert description. */
	oldContext = MemoryContextSwitchTo(TopMemoryContext);
	insertDesc->upperPartitionLevel = partitionLevel;
	insertDesc->targetPartitionLevel = targetPartitionLevel;

	insertDesc->nprivatebuffer = (int) 
		pow(meta->spreadFactor, targetPartitionLevel - partitionLevel);
	insertDesc->targetPartitionCoverage = (int)
		pow(meta->spreadFactor, meta->lastPartitionLevel - targetPartitionLevel);

	if (partitionLevel == 0)
		insertDesc->startTargetPartitionNumber = 0;
	else
		insertDesc->startTargetPartitionNumber = 
			minPartitionNumber * insertDesc->nprivatebuffer;

	/* Allocate private buffers. */
	insertDesc->privateBuffers = (PandoraPartitionPrivateBuffer **)
		palloc0(sizeof(PandoraPartitionPrivateBuffer *) * 
								insertDesc->nprivatebuffer);

	for (int i = 0; i < insertDesc->nprivatebuffer; ++i)
	{
		PandoraPartitionPrivateBuffer *privateBuffer = 
			(PandoraPartitionPrivateBuffer *)
				palloc0(sizeof(PandoraPartitionPrivateBuffer));

		/* 
		 * Set partition number and partition generation number to append 
		 * tuples. 
		 */
		privateBuffer->partitionNumber = 
			insertDesc->startTargetPartitionNumber + i;
		privateBuffer->partitionGenerationNumber = 
			PandoraGetPartGenerationNumber(meta, 
				insertDesc->targetPartitionLevel, 
				privateBuffer->partitionNumber);

		privateBuffer->firstRecCnt = 
			meta->realRecordsCount[targetPartitionLevel][privateBuffer->partitionNumber];
		privateBuffer->currentRecCnt = privateBuffer->firstRecCnt;
		privateBuffer->partial = false;

		/* If a new file should be created, set the next generation number. */
		if (privateBuffer->firstRecCnt == 0 &&
				insertDesc->targetPartitionLevel != meta->lastPartitionLevel)
		{
			/* Move to next generation number. */
			if (privateBuffer->partitionGenerationNumber == 
													InvalidPandoraPartGenNo)
				privateBuffer->partitionGenerationNumber = 0;
			else
				privateBuffer->partitionGenerationNumber += 1;
		}

		/* Init page array */
		privateBuffer->pages = 
			(Page *) palloc0(sizeof(Page) * PRIVATEBUFFER_INIT_CAPACITY);
		privateBuffer->npage = 0;
		privateBuffer->capacity = PRIVATEBUFFER_INIT_CAPACITY;
		privateBuffer->nblocks = PandoraNblocks(desc->relation->rd_node,
									insertDesc->targetPartitionLevel,
									privateBuffer->partitionNumber,
									privateBuffer->partitionGenerationNumber);
		
		insertDesc->privateBuffers[i] = privateBuffer;
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * PandoraWritePartialPage
 * 
 * At the end of repartitioning, we write partial page to the shared memory.
 * We write only modified space, because there are existing tuples. Some workers
 * can access to existing tuples, so we get the exclusive lock on the page. 
 */
static void
PandoraWritePartialPage(Relation relation, PandoraRelationMeta *meta, 
								PandoraPartLevel targetPartitionLevel,
								PandoraPartitionPrivateBuffer *privateBuffer)
{
	PageHeader pageHeader = (PageHeader) privateBuffer->pages[0];
	BlockNumber lastBlockNumber;
	Buffer 		buffer;
	PageHeader	dp;

	Assert(privateBuffer->firstRecCnt != 0);

	/* Get the partition's last page before appending tuples. */
	lastBlockNumber = 
		(privateBuffer->firstRecCnt - 1) / meta->recordNumsPerBlock;

	/* Read partial page. */
	buffer = ReadPartitionBufferForRepartitioning(relation, 
												  targetPartitionLevel, 
												  privateBuffer->partitionNumber,
												  privateBuffer->partitionGenerationNumber,
												  lastBlockNumber, RBM_NORMAL, NULL);

	/* Lock the buffer. */
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	dp = (PageHeader) BufferGetPage(buffer);

	Assert(privateBuffer->prevLower <= pageHeader->pd_lower);
	Assert(pageHeader->pd_upper <= privateBuffer->prevUpper);

	/* Copy line items to the shared memory's buffer. */
	memcpy((char *) dp + privateBuffer->prevLower, 
				(char *) pageHeader + privateBuffer->prevLower,
				pageHeader->pd_lower - privateBuffer->prevLower);

	/* Copy tuples to the shared memory's buffer. */
	memcpy((char *) dp + pageHeader->pd_upper, 
				(char *) pageHeader + pageHeader->pd_upper,
				privateBuffer->prevUpper - pageHeader->pd_upper);

	/* Adjust lower and upper bound of the page. */
	dp->pd_lower = pageHeader->pd_lower;
	dp->pd_upper = pageHeader->pd_upper;

	MarkBufferDirty(buffer);

	/* Unlock and unpin the buffer. */
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
}

/*
 * PandoraWriteRemainPages
 *
 * The repartitioning worker writes the remaining pages stored in private memory 
 * to the partition file. To minimize the number of system calls, we utilize the 
 * 'pwritev' system call.
 */
static void
PandoraWriteRemainPages(Relation relation, PandoraRelationMeta *meta, 
								PandoraPartLevel targetPartitionLevel,
								PandoraPartitionPrivateBuffer *privateBuffer)
{
	int startPageIdx = privateBuffer->partial ? 1 : 0;
	int64_t pageNum = privateBuffer->npage - startPageIdx;
	int iovsIdx = 0;
	int pageIdx = 0;
	struct iovec *iovs;
	uint64_t offset;
	BlockNumber firstBlockNum;

	/* If we don't have any remain page to write, just return. */
	if (pageNum == 0)
		return;

	/* Allocate i/o vector. */
	iovs = (struct iovec *) palloc0(sizeof(struct iovec) * pageNum);

	/* Set values within i/o vector. */
	for (pageIdx = startPageIdx, iovsIdx = 0; pageIdx < privateBuffer->npage; 
															++pageIdx, ++iovsIdx)
	{
		iovs[iovsIdx].iov_base = (void *) privateBuffer->pages[pageIdx];
		iovs[iovsIdx].iov_len = BLCKSZ;
	}

	/* Get block number to start appending tuples. */
	if (privateBuffer->firstRecCnt == 0)
	{
		Assert(privateBuffer->partial == false);
		firstBlockNum = 0;
	}
	else
	{
		firstBlockNum = 
			((privateBuffer->firstRecCnt - 1) / meta->recordNumsPerBlock) + 1;
	}

	/* File offset to write. */
	offset = ((uint64_t) firstBlockNum) * BLCKSZ;

	/* Write remain pages. */
	PandoraWriteBatch(relation->rd_node, targetPartitionLevel, 
						privateBuffer->partitionNumber,
						privateBuffer->partitionGenerationNumber,
						offset, iovs, iovsIdx);

	/* Resource cleanup. */
	pfree(iovs);
}

/*
 * PandoraEndRepartitioning
 *
 * At the end of repartitioning, we write the pages stored in private memory 
 * either to shared memory or directly to a file. Additionally, we perform 
 * resource cleanup tasks, such as 'pfree'. 
 */
static void
PandoraEndRepartitioning(PandoraRepartitioningDesc *desc)
{
	PandoraPartitionScanDesc *upperScan = &(desc->upperScan);
	PandoraPartitionInsertDesc *insertDesc = &(desc->lowerInsert);
	Relation relation = desc->relation;
	PandoraRelationMeta *meta = desc->meta;
	pg_atomic_uint64 *refcounter;

	/* Get upper partition's reference counter pointer. */
	if (upperScan->partitionLevel == 0)
		refcounter = PandoraRelationMetaRefCounter(meta, 0, 0);
	else
		refcounter = 
			PandoraRelationMetaRefCounter(meta, 
				upperScan->partitionLevel, upperScan->currentPartitionNumber);

#ifdef PANDORA_MEASURE
	clock_gettime(CLOCK_MONOTONIC, &desc->copyEndTime);
#endif

	/*
	 * Repartitioning at checkpoint is executed when the PostgreSQL server shuts
	 * down, so no delta is created.
	 */
	if (likely(upperScan->isPartitioningWorker))
	{
		// TODO: remove
		//pg_usleep(1000 * 1000 * 10);

		/* 
		 * The simple memory copy phase is done. Now, we need to fix 
		 * uncorrected tuples within private buffers. 
		 */
		SetRepartitioningStatusModificationBlocking(refcounter);
	}

#ifdef PANDORA_MEASURE
	clock_gettime(CLOCK_MONOTONIC, &desc->blockingStartTime);
#endif

	if (likely(upperScan->isPartitioningWorker))
	{
		/* Apply delta to uncorrected tuples. */
		ApplyPandoraDeltas(pandoraPartitionDsaArea, desc);
	}

	/* Iterate all private buffer to write. */
	for (int idx = 0; idx < insertDesc->nprivatebuffer; ++idx)
	{
		PandoraPartitionPrivateBuffer *privateBuffer = 
											insertDesc->privateBuffers[idx];
		int prevBlockNum, curBlockNum;

		if (privateBuffer->firstRecCnt == 0)
		{
			/* Create partition file if not exist. */
			if (privateBuffer->currentRecCnt != 0)
			{
				PandoraCreatePartitionFile(relation->rd_node, 
							insertDesc->targetPartitionLevel,
							privateBuffer->partitionNumber,
							privateBuffer->partitionGenerationNumber);
			}

			/* Get prev block number. */
			prevBlockNum = -1;
		}
		else
		{
			if (privateBuffer->partial)
			{
				/* If there is partial page to write, we write that page first. */
				PandoraWritePartialPage(relation, meta, 
								insertDesc->targetPartitionLevel, privateBuffer);
			}

			/* Get prev block number. */
			prevBlockNum = 
				(privateBuffer->firstRecCnt - 1) / meta->recordNumsPerBlock;
		}

		pg_memory_barrier();

		/* Write remain pages calling one pwritev. */
		PandoraWriteRemainPages(relation, meta,
								insertDesc->targetPartitionLevel, privateBuffer);

		/* Get current block number. */
		if (privateBuffer->currentRecCnt == 0)
			curBlockNum = -1;
		else
			curBlockNum = 
				(privateBuffer->currentRecCnt - 1) / meta->recordNumsPerBlock;

		/* 
		 * Increment the number of blocks that we created for child partitions. 
		 */
		desc->childBlocks += (curBlockNum - prevBlockNum);

#ifdef PANDORA_DEBUG
		fprintf(stderr, "[PANDORA] PandoraEndRepartitioning, idx: %d, prev: %d, curr: %d, added block amount: %u (relid: %d, level: %, number: %)\n",
						idx, prevBlockNum, curBlockNum, (curBlockNum - prevBlockNum), 
						relation->rd_node.relNode, insertDesc->targetPartitionLevel, privateBuffer->partitionNumber);
#endif /* PANDORA DEBUG */
	}

	/*
	 * Checkpointer uses backed up relation data, so it didn't open any
	 * relation.
	 */
	if (likely(upperScan->isPartitioningWorker))
	{
		/* Close relation. */
		table_close(relation, AccessShareLock);
	}

	/* Resource cleanup. */
	for (int idx = 0; idx < insertDesc->nprivatebuffer; ++idx)
	{
		PandoraPartitionPrivateBuffer *privateBuffer = 
											insertDesc->privateBuffers[idx];
	
		/* Page cleanup. */
		for (int pageIdx = 0; pageIdx < privateBuffer->npage; ++pageIdx)
			pfree(privateBuffer->pages[pageIdx]);
		
		/* Private buffer cleanup. */
		pfree(privateBuffer->pages);
		pfree(privateBuffer);
	}

	pfree(insertDesc->privateBuffers);
}

/*
 * PandoraGetTupleFromPartitions
 */
static SiroTuple
PandoraGetTupleFromPartitions(PandoraRepartitioningDesc *desc)
{
	PandoraPartitionScanDesc *pscan = &(desc->upperScan);
	SiroTuple tuple;

	for (;;)
	{
		/*
		 * Get a tuple to do repartitioning into downside partition.
		 */
		tuple = PandoraPartitionGetTuple(pscan);

		if (tuple == NULL)
		{
#ifdef PANDORA_DEBUG
			fprintf(stderr, "[PANDORA] one partition was repartitioned, level: %u, number: %lu, ntuple: %lu\n",
							pscan->partitionLevel, pscan->currentPartitionNumber, pscan->repartitionedNtuple_partition);
			if (pscan->partitionLevel == 0 && pscan->isPartitioningWorker &&
				pscan->repartitionedNtuple_partition < desc->meta->recordNumsPerBlock * 2048)
			{
				char errmsg[32];
				sprintf(errmsg, "cur page: %u, nblocks: %u", pscan->currentBlock, pscan->nblocks);
				Abort(errmsg);
			}
			pscan->repartitionedNtuple_partition = 0;
#endif /* PANDORA DEBUG */
			if (pscan->currentPartitionNumber < pscan->maxPartitionNumber)
			{
				/* Set the next partition scan descriptor. */
				pscan->currentPartitionNumber += 1;
				pscan->currentPartitionGenerationNumber = 
									PandoraGetPartGenerationNumber(desc->meta, 
											pscan->partitionLevel,
											pscan->currentPartitionNumber);

				/*
				 * Set page count of current partition.
				 * (See PandoraBeginRepartitioning() for details)
				 */
				if (likely(pscan->isPartitioningWorker))
				{
					/* Normal case: partitioning worker */
					pscan->nblocks = pscan->partitionLevel == 0 ?
						PandoraMempartPageCount :
						NBlocksUsingRecordCount(desc->meta->realRecordsCount[pscan->partitionLevel][pscan->currentPartitionNumber],
												desc->meta->recordNumsPerBlock);
				}
				else
				{
					/* PostgreSQL server is shutting down: checkpointer */
					Assert(pscan->partitionLevel == 0);

					pscan->nblocks = (pscan->currentPartitionNumber == desc->meta->partitionNumberLevelZero[1]) ?
						NBlocksUsingRecordCount(desc->meta->recordCountsLevelZero[0],
												desc->meta->recordNumsPerBlock) :
						PandoraMempartPageCount;
				}

				pscan->startBlock = 0;
				pscan->firstScan = true;

#ifdef PANDORA_DEBUG
				fprintf(stderr, "[PANDORA] PandoraGetTupleFromPartitions, level: %u, number: %u, nblocks: %u\n",
								pscan->partitionLevel, pscan->currentPartitionNumber, pscan->nblocks);
#endif /* PANDORA DEBUG */

				desc->parentBlocks += pscan->nblocks;
			}
			else
			{
				/* End scan */
				return NULL;
			}
		}
		else
		{
			return tuple;
		}
	}

	Assert(false);
	/* quiet compiler */
	return NULL;
}


/*
 * PandoraRepartitioning - repartitioning main entry
 * 
 * We redistribute upper partition's tuples to lower partitions. When we get 
 * tuples from upper partition, we access to shared buffer. When we move tuples 
 * to lower partitions, we save tuples to private memory.
 *
 * (level)
 *    n     ▢▢▢                 <-- upper partition
 *           ↓---↘----↘----↘
 *   n+1    ▢▢▢  ▢▢▢  ▢▢▢  ▢▢▢  <-- lower partitions
 */
void
PandoraRepartitioning(PandoraRepartitioningDesc *desc, Oid relationOid,
					  uint64 recordCount,
					  PandoraPartLevel partitionLevel,
					  PandoraPartNumber minPartitionNumber,
					  PandoraPartNumber maxPartitionNumber,
					  PandoraPartLevel targetPartitionLevel,
					  uint32 *reorganizedRecCnt,
					  PandoraSeqNum *firstSeqNums, bool isPartitioningWorker)
{
	uint32 repartitionedNtuple = 0;
	SiroTuple upperTuple;
	uint32 childIndex;

	/* Init repartitioning descriptor. */
	PandoraBeginRepartitioning(desc, relationOid, partitionLevel, 
							   minPartitionNumber, maxPartitionNumber, 
							   targetPartitionLevel, isPartitioningWorker);
	
	for (;;)
	{
		/* Retrieve a tuple from the target partitions. */
		upperTuple = PandoraGetTupleFromPartitions(desc);

		if (upperTuple == NULL)
		{
#ifdef PANDORA_DEBUG
			if (repartitionedNtuple != recordCount)
			{
				char errmsg[32];
				sprintf(errmsg, "repartitionedNtuple != recordCount (%u, %lu)\n",
						repartitionedNtuple, recordCount);
				Abort(errmsg);
			}
#else /* !PANDORA_DEBUG */
			Assert(repartitionedNtuple == recordCount);
#endif /* PANDORA_DEBUG */

			/* When the return value is NULL, the scan is complete. */
			PandoraEndRepartitioning(desc);

			if (repartitionedNtuple > 0)
				fprintf(stderr, 
					"[PANDORA] %d repartitioning end (level: %d, number: %d ~ %d) ntuple: %u, ndelta: %lu, conflict: %s\n",
							desc->meta->tag.relationOid,
							partitionLevel, minPartitionNumber, 
							maxPartitionNumber, repartitionedNtuple,
							desc->deltaNums,
							desc->deltaNums == 0 ? "false" : "true");

			return;
		}

		repartitionedNtuple += 1;

		/* 
		 * The retrieved tuple is added to the lower partitions at the target 
		 * level. 
		 */
		childIndex = PandoraPartitionAppend(desc, upperTuple, firstSeqNums);
		pg_memory_barrier();

		/* Memoize the count of repartitioned records */
		reorganizedRecCnt[childIndex]++;
	}
}

/* ----------------------------------------------------------------
 *			 pandora partition access method interface
 * ----------------------------------------------------------------
 */

/*
 * pandorapartgetpage - subroutine for PandoraPartitionGetTuple()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 */
void
PandoraPartitionGetPage(PandoraPartitionScanDesc *pscan, BlockNumber page)
{
	Relation	relation = pscan->relation;
	Buffer		buffer;
	Page		dp;
	PageHeader	phdr;
	int			lines;
	int			ntup;
	OffsetNumber lineoff;
	ItemId		lpp;
	Item		pLocator;
	int			i;
	HeapTupleData loctup;

	Assert(page < pscan->nblocks);

	/* release previous scan buffer, if any */
	if (BufferIsValid(pscan->currentBuffer))
	{
		UnlockReleaseBuffer(pscan->currentBuffer);
		pscan->currentBuffer = InvalidBuffer;

		for (i = 0; i < pscan->pinOtherBufferNums; ++i)
		{
			ReleaseBuffer(pscan->tupleBuffers[i]);
			pscan->tupleBuffers[i] = InvalidBuffer;
		}
		
		pscan->pinOtherBufferNums = 0;
	}

	/*
	 * Be sure to check for interrupts at least once per page.  Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

	/* read page using selected strategy */
	pscan->currentBuffer =
		ReadPartitionBufferForRepartitioning(pscan->relation, 
											 pscan->partitionLevel, 
											 pscan->currentPartitionNumber, 
											 pscan->currentPartitionGenerationNumber,
											 page, RBM_NORMAL, NULL);
	pscan->currentBlock = page;

	buffer = pscan->currentBuffer;

	/*
	 * This prevents repartitioning a mempartition where no actual record has
	 * been inserted yet.
	 * Repartitioning immediately after tiering while records have not yet been
	 * inserted will cause problems, so ensure that all records in the tiered
	 * mempartition are inserted before proceeding with repartitioning.
	 * In practice, this is very unlikely to happen, and in fact, the above
	 * problem has never occurred even when we didn't add this code.
	 * However, it is theoretically possible, so we add this code to ensure
	 * stability.
	 */
	if (unlikely(PandoraCheckMempartBuf(buffer - 1) && page == pscan->startBlock &&
				 pscan->isPartitioningWorker))
	{
#ifdef PANDORA_DEBUG
		fprintf(stderr, "[PANDORA] mempart, rel: %u, cnt: %u, buf: %p, pid: %d\n",
						pscan->relation->rd_id, GetInsertedRecordsCount(buffer),
						GetBufferDescriptor(PandoraGetMempartBufId(buffer - 1)),
						(int)getpid());
#endif /* PANDORA_DEBUG */
		WaitForInsert(buffer, pscan);
	}
	
	loctup.t_tableOid = relation->rd_node.relNode;

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.  Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = BufferGetPage(buffer);
	lines = PageGetMaxOffsetNumber(dp);
	ntup = 0;
	phdr = (PageHeader)dp;

	for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
		 lineoff <= lines;
		 lineoff++, lpp++)
	{
		SiroTuple	versionLocTup;
		uint32	   *p_pd_gen;

		/* Don't read version directly */
		if (!LP_IS_PLEAF_FLAG(lpp))
			continue;

		/* Get p-locator of this record */
		pLocator = PageGetItem(dp, lpp);
		p_pd_gen = (uint32*)((char *) pLocator + PITEM_GEN_OFF);

		versionLocTup = &(pscan->siroTuples[ntup]);

		versionLocTup->plocatorData = pLocator;
		versionLocTup->plocatorLen = ItemIdGetLength(lpp);
		versionLocTup->leftVersionData = NULL;
		versionLocTup->leftVersionLen = 0;
		versionLocTup->rightVersionData = NULL;
		versionLocTup->rightVersionLen = 0;

		Assert(versionLocTup->plocatorLen == PITEM_SZ);

		/* Check pd_gen */
		if (*p_pd_gen == phdr->pd_gen)
			versionLocTup->isUpdated = true;
		else
			versionLocTup->isUpdated = false;

		for (int veroff = CHECK_LEFT; veroff <= CHECK_RIGHT; ++veroff)
		{
			OffsetNumber	versionOffset = InvalidOffsetNumber;
			ItemPointer		versionPointer;
			BlockNumber		versionBlock;
			Buffer			versionBuffer;
			Page			versionDp;
			ItemId			versionLpp;

			versionPointer = PGetPointer(pLocator, veroff);

			/* Set block number and offset */
			versionBlock = ItemPointerGetBlockNumber(versionPointer);
			versionOffset = ItemPointerGetOffsetNumber(versionPointer);
			Assert(versionBlock == page && versionOffset != 0);

			/* Set self Pointer of tuple */
			ItemPointerSet(&(loctup.t_self), versionBlock, versionOffset);

			/*
			 * If the version is in the same page with p-locator, just get it.
			 * Or not, read the buffer that it is in.
			 */
			if (likely(versionBlock == page))
			{
				versionBuffer = buffer;
				versionDp = dp;
			}
			else
			{
				/* XXX: We are not serving appended update yet. */
				Assert(false);
				// versionBuffer = ReadBuffer(pscan->relation, versionBlock);
				// Assert(BufferIsValid(versionBuffer));
				// versionDp = BufferGetPage(versionBuffer);

				// pscan->tupleBuffers[pscan->pinOtherBufferNums] = versionBuffer;
				// pscan->pinOtherBufferNums += 1;
			}

			versionLpp = PageGetItemId(versionDp, versionOffset);

			/* The target has never been updated after INSERT */
			if (unlikely(LP_OVR_IS_UNUSED(versionLpp)))
				continue;

			if (veroff == CHECK_LEFT)
			{
				versionLocTup->leftVersionData = 
						(HeapTupleHeader) PageGetItem(versionDp, versionLpp);
				versionLocTup->leftVersionLen = ItemIdGetLength(versionLpp);
				Assert(versionLocTup->leftVersionData->t_choice.t_heap.t_xmin != 
														InvalidTransactionId);
	
				if (versionLocTup->leftVersionLen != 0)
				{
					loctup.t_data = versionLocTup->leftVersionData; 
					loctup.t_len = versionLocTup->leftVersionLen;

					(void) HeapTupleSetHintBitsForPartition(&loctup, buffer);
				}
			}
			else if (veroff == CHECK_RIGHT)
			{
				versionLocTup->rightVersionData = 
						(HeapTupleHeader) PageGetItem(versionDp, versionLpp);
				versionLocTup->rightVersionLen = ItemIdGetLength(versionLpp);
				Assert(versionLocTup->rightVersionData->t_choice.t_heap.t_xmin != 
															InvalidTransactionId);

				if (versionLocTup->rightVersionLen != 0)
				{
					loctup.t_data = versionLocTup->rightVersionData; 
					loctup.t_len = versionLocTup->rightVersionLen;

					(void) HeapTupleSetHintBitsForPartition(&loctup, buffer);
				}
			}
		}

		ntup += 1;
	}

	Assert(ntup == lines/3);
	Assert(ntup <= MaxHeapTuplesPerPage);
	pscan->ntuples = ntup;

#ifdef PANDORA_DEBUG
	if (ntup == 0 || ntup != lines/3)
	{
		char errmsg[32];
		sprintf(errmsg, "ntup: %d, lines/3: %d", ntup, lines/3);
		Abort(errmsg);
	}

	pscan->repartitionedNtuple_partition += ntup;
#endif /* PANDORA_DEBUG */
}

/*
 * PandoraPartitionGetTuple
 */
SiroTuple
PandoraPartitionGetTuple(PandoraPartitionScanDesc *pscan)
{
	BlockNumber page;
	bool		finished;
	int			lines;
	int			lineindex;
	int			linesleft;
	int			i;

	/*
	 * calculate next starting lineindex.
	 */
	if (pscan->firstScan)
	{
		/*
		 * return null immediately if relation is empty
		 */
		if (pscan->nblocks == 0)
		{
			Assert(!BufferIsValid(pscan->currentBuffer));
			pscan->currentTuple = NULL;
			return NULL;
		}
		else
		{
			page = pscan->startBlock; /* first page */
		}

		PandoraPartitionGetPage(pscan, page);
		lineindex = 0;
		pscan->firstScan = false;
	}
	else
	{
		/* continue from previously returned page/tuple */
		page = pscan->currentBlock; /* current page */
		lineindex = pscan->currentIndex + 1;
	}

	lines = pscan->ntuples;

	/* page and lineindex now reference the next visible tid */
	linesleft = lines - lineindex;

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	for (;;)
	{
		if (linesleft > 0)
		{
			pscan->currentTuple = &(pscan->siroTuples[lineindex]);
			pscan->currentIndex = lineindex;

			Assert(pscan->currentTuple->plocatorLen == PITEM_SZ);
			Assert(pscan->currentTuple->leftVersionData->t_choice.t_heap.t_xmin != InvalidTransactionId);
			Assert(pscan->currentTuple->rightVersionData == NULL || pscan->currentTuple->rightVersionData->t_choice.t_heap.t_xmin != InvalidTransactionId);

			return pscan->currentTuple;
		}

		if (++page >= pscan->nblocks)
			page = 0;
		finished = (page == pscan->startBlock);

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(pscan->currentBuffer))
				UnlockReleaseBuffer(pscan->currentBuffer);

			for (i = 0; i < pscan->pinOtherBufferNums; ++i)
			{
				ReleaseBuffer(pscan->tupleBuffers[i]);
				pscan->tupleBuffers[i] = InvalidBuffer;
			}

			pscan->currentBuffer = InvalidBuffer;
			pscan->currentBlock = InvalidBlockNumber;
			pscan->currentTuple = NULL;
			pscan->firstScan = false;
			return NULL;
		}

		PandoraPartitionGetPage(pscan, page);

		lines = pscan->ntuples;
		linesleft = lines;
		lineindex = 0;
	}
}

/*
 * EnlargePrivateBufferPageArray
 */
static void
EnlargePrivateBufferPageArray(PandoraPartitionPrivateBuffer *privateBuffer)
{
	Page *oldPageArray = privateBuffer->pages;
	Page *newPageArray;
	int newCapacity;

	if (privateBuffer->npage < privateBuffer->capacity)
		return;

	/* Doubling */
	newCapacity = 2 * privateBuffer->capacity;
	newPageArray = (Page *) palloc0(sizeof(Page) * newCapacity);

	/* Copy elements of old array. */
	memcpy(newPageArray, oldPageArray, sizeof(Page) * privateBuffer->npage);
	pfree(oldPageArray);

	/* Set new array. */
	privateBuffer->pages = newPageArray;
	privateBuffer->capacity = newCapacity;
}

/*
 * PandoraPartitionGetPrivateBufferForTuple
 *
 * We find target page to append tuple.
 */
static PandoraPartitionPrivateBuffer *
PandoraPartitionGetPrivateBufferForTuple(PandoraRepartitioningDesc *desc, 
														Size len, Page *page)
{
	PandoraPartitionInsertDesc *insertDesc = &(desc->lowerInsert);
	PandoraRelationMeta	*meta = desc->meta;
	Index		privateBufferIdx;
	PandoraPartitionPrivateBuffer *privateBuffer;

	/*
	 * If we're gonna fail for oversize tuple, do it right away
	 */
	if (len > MaxHeapTupleSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %zu, maximum size %zu",
					 len, MaxHeapTupleSize)));

	/* Get index on private buffers to append the tuple. */
	privateBufferIdx = 
		insertDesc->targetPartitionNumber % insertDesc->nprivatebuffer;
	privateBuffer = insertDesc->privateBuffers[privateBufferIdx];
	Assert(insertDesc->targetPartitionNumber == privateBuffer->partitionNumber);

	/* Enlarge page array. */
	EnlargePrivateBufferPageArray(privateBuffer);
	Assert(privateBuffer->npage < privateBuffer->capacity);

	/* Get page from private buffers. */
	if (unlikely(privateBuffer->npage == 0))
	{
		/* Initially appending to the partition. */
	
		/* Allocate new page, using local memory. */
		*page = (Page) palloc(BLCKSZ);

		privateBuffer->pages[privateBuffer->npage] = *page;
		privateBuffer->npage += 1;

		/* Init the new page. */
		PageInit(*page, BLCKSZ, 0);

		if (unlikely((privateBuffer->firstRecCnt % 
									meta->recordNumsPerBlock) != 0))
		{
			PageHeader pageHeader = (PageHeader) *page;

			/* This is partial page case. We set lower and upper offset. */
			pageHeader->pd_lower += 
				sizeof(ItemIdData) * 3 *
					(privateBuffer->firstRecCnt % meta->recordNumsPerBlock);
			pageHeader->pd_upper -= 
				len * (privateBuffer->firstRecCnt % meta->recordNumsPerBlock);

			/* Save lower and upper info to the private buffer. */
			privateBuffer->partial = true;
			privateBuffer->prevLower = pageHeader->pd_lower;
			privateBuffer->prevUpper = pageHeader->pd_upper;
		}
	}
	else
	{
		if (unlikely((privateBuffer->currentRecCnt % 
											meta->recordNumsPerBlock) == 0))
		{
			/* Allocate new page, using local memory. */
			*page = (Page) palloc(BLCKSZ);

			privateBuffer->pages[privateBuffer->npage] = *page;
			privateBuffer->npage += 1;

			/* Init the new page. */
			PageInit(*page, BLCKSZ, 0);
		}
		else
		{
			/* 
			 * We can use last private page in private buffer. That page is 
			 * already made by previous append operation.
			 */
			*page = privateBuffer->pages[privateBuffer->npage - 1];
		}
	}

	/* The page should be inited status. If not, that is an error.  */
	if (PageIsNew(*page))
		elog(ERROR, "page of relation \"%s\" should not be empty",
				RelationGetRelationName(desc->relation));

	Assert(*page != NULL);

	return privateBuffer;
}

/*
 * PageAddItemExtendedWithDummy
 * 
 * We add siro tuple to the page.
 */
static OffsetNumber
PandoraPartitionPageAddItem(Page page, BlockNumber blockNum,
								SiroTuple siroTuple, Size maxlen)
{
	PageHeader	phdr = (PageHeader) page;
	ItemId		plocatorItemId, leftVersionItemId, rightVersionItemId;
	char	   *p_pointer;
	Size		p_alignedSize;
	int			lower;
	int			upper;
	OffsetNumber insertionOffsetNumber;
	uint32	   *p_pd_gen;

	/*
	 * Be wary about corrupted page pointers
	 */
	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > BLCKSZ)
	{
		ereport(PANIC,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
				 phdr->pd_lower, phdr->pd_upper, phdr->pd_special)));
	}

	/*
	 * Select offsetNumber to place the new item at
	 */
	insertionOffsetNumber = OffsetNumberNext(PageGetMaxOffsetNumber(page));

	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	p_alignedSize = MAXALIGN(PITEM_SZ);

	lower = phdr->pd_lower + sizeof(ItemIdData) * 3;
	upper = (int) phdr->pd_upper - (int) maxlen * 2 - (int) p_alignedSize;

	if (lower > upper)
	{
		elog(WARNING, "lower > upper");
		return InvalidOffsetNumber;
	}

	/*
	 * OK to append the item.
	 */
	
	/* Set the line pointer for p-locator */
	plocatorItemId = PageGetItemId(phdr, insertionOffsetNumber);
	ItemIdSetNormal(plocatorItemId, upper, PITEM_SZ);
	LP_OVR_SET_LEFT(plocatorItemId);
	LP_OVR_SET_UNUSED(plocatorItemId);
	LP_SET_PLEAF_FLAG(plocatorItemId);

	/* set the line pointer of left version*/
	leftVersionItemId = PageGetItemId(phdr, insertionOffsetNumber + 1);
	ItemIdSetNormal(leftVersionItemId,
					upper + (int) p_alignedSize, siroTuple->leftVersionLen);
	LP_OVR_SET_USING(leftVersionItemId);

	/* Set the line pointer of right version */
	rightVersionItemId = PageGetItemId(phdr, insertionOffsetNumber + 2);
	ItemIdSetNormal(rightVersionItemId, 
					upper + ((int) p_alignedSize + (int) maxlen), 
					siroTuple->rightVersionLen);
	LP_OVR_SET_UNUSED(rightVersionItemId);

	/* Zero the p-locator */
	p_pointer = (char *) page + upper;
	Assert(PITEM_SZ <= p_alignedSize);
	memcpy(p_pointer, siroTuple->plocatorData, (Size) PITEM_SZ);

	/* Copy the item's data onto the page (not onto dummy) */
	p_pointer += (int) p_alignedSize;
	Assert(siroTuple->leftVersionLen <= maxlen);
	if (siroTuple->leftVersionLen > 0)
		memcpy(p_pointer, siroTuple->leftVersionData, siroTuple->leftVersionLen);

	/* Zero the right version */
	p_pointer += (int) maxlen;
	Assert(siroTuple->rightVersionLen <= maxlen);
	if (siroTuple->rightVersionLen > 0)
		memcpy(p_pointer, siroTuple->rightVersionData, siroTuple->rightVersionLen);

	/* Set info of p-locator (see architecture in bufpage.h) */
	p_pointer = (char *) page + upper + PITEM_GEN_OFF;
	p_pd_gen = (uint32*)p_pointer;
	p_pointer += sizeof(uint64); /* uint32 + uint16 + uint16 */
	ItemPointerSet((ItemPointer)p_pointer, blockNum, insertionOffsetNumber + 1);
	p_pointer += sizeof(ItemPointerData);
	ItemPointerSet((ItemPointer)p_pointer, blockNum, insertionOffsetNumber + 2);
	p_pointer += sizeof(ItemPointerData);

	/* Set pd_gen */
	if (siroTuple->isUpdated)
		*p_pd_gen = phdr->pd_gen;
	else
		*p_pd_gen = phdr->pd_gen - 1;

	/* adjust page header */
	phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;

	return insertionOffsetNumber;
}

/*
 * PandoraPartitionPutSiroTuple
 *
 * Place a siro tuple at specified page.
 */
static void
PandoraPartitionPutSiroTuple(Relation relation, BlockNumber blockNum, 
									Page pageHeader, SiroTuple siroTuple)
{
	TupleDesc tupDesc = RelationGetDescr(relation);
	OffsetNumber offnum;
	
	/* Add the tuple to the page */
	offnum = PandoraPartitionPageAddItem(pageHeader, blockNum, 
											siroTuple, tupDesc->maxlen);

	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "failed to add tuples to page");
}

/*
 * GetPartitionNumberFromPartitionId
 */
PandoraPartNumber
GetPartitionNumberFromPartitionId(int spreadFactor, PandoraPartLevel destLevel, 
									PandoraPartLevel lastLevel, int partitionId)
{
	int coverage;

	Assert(destLevel <= lastLevel);
	coverage = (int) pow(spreadFactor, lastLevel - destLevel);

	return (PandoraPartNumber) (partitionId / coverage);
}

/*
 * PandoraPartitionAppend
 *
 * In the repartitioning process, the tuple will be appended to the lower 
 * partitions at the target level.
 */
uint32
PandoraPartitionAppend(PandoraRepartitioningDesc *desc, SiroTuple tuple,
					   PandoraSeqNum *firstSeqNums)
{
	PandoraRelationMeta *meta = desc->meta;
	PandoraPartitionInsertDesc *insertDesc = &(desc->lowerInsert);
	TupleDesc	tupleDesc = RelationGetDescr(desc->relation);
	BlockNumber blockNum;
	PandoraPartitionPrivateBuffer *privateBuffer;
	PandoraRecordKey *recordKey;
	int 		partitionId;
	Page		page;
	uint32		childIndex;

	/* Cheap, simplistic check that the tuple matches the rel's rowtype. */
	Assert(HeapTupleHeaderGetNatts(tuple->leftVersionData) <= tupleDesc->natts);

	/* Get a partition ID using the record key from the p-locator. */
#ifdef PANDORA_DEBUG
	if (tuple->plocatorLen != PITEM_SZ)
	{
		char errmsg[32];
		sprintf(errmsg, "tuple->plocatorLen != PITEM_SZ (%u)\n",
						tuple->plocatorLen);
		Abort(errmsg);
	}
#else /* !PANDORA_DEBUG */
	Assert(tuple->plocatorLen == PITEM_SZ);
#endif /* PANDORA_DEBUG */

	recordKey = PGetPandoraRecordKey(tuple->plocatorData);
	partitionId = recordKey->partid;

	/* Set partition number to append tuple. */
	insertDesc->targetPartitionNumber = 
						GetPartitionNumberFromPartitionId(meta->spreadFactor, 
										insertDesc->targetPartitionLevel, 
										meta->lastPartitionLevel, partitionId);
	
	childIndex = insertDesc->targetPartitionNumber % meta->spreadFactor;
	
	pg_memory_barrier();

	/* Memoize first record sequence number */
	if (unlikely(firstSeqNums[childIndex] == InvalidPandoraSeqNum))
		firstSeqNums[childIndex] = recordKey->seqnum;

	/* Get private memory space to appdend the tuple. */
	privateBuffer = PandoraPartitionGetPrivateBufferForTuple(desc,
						(MAXALIGN(PITEM_SZ) + 2 * tupleDesc->maxlen), &page);

	/* Calculate current block number. */
	blockNum = privateBuffer->currentRecCnt / meta->recordNumsPerBlock;

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	/* Append the tuple to the page of the private buffer. */
	PandoraPartitionPutSiroTuple(desc->relation, blockNum, page, tuple);
	privateBuffer->currentRecCnt += 1;

	END_CRIT_SECTION();

	return childIndex;
}

/*
 * RepartitionNewestMempartition
 *
 * When the PostgreSQL server shuts down, checkpointer repartitions the newest
 * mempartition remaining in the buffer.
 */
void
RepartitionNewestMempartition(int flags)
{
	Oid	relationOid;
	PandoraRelationMeta *meta;
	PandoraRepartitioningDesc desc;
	PandoraPartNumber	minPartitionNumber;
	PandoraPartNumber	maxPartitionNumber;
	uint64				level0RecordCount;
	uint32				reorganizedRecCnt[32];
	PandoraSeqNum		firstSeqNums[32];
	char				oldFileName[32];

	/*
	 * Repartitioning for the newest mempartition should only be executed at
	 * PostgreSQL server shutdown.
	 */
	if (likely(!(flags & CHECKPOINT_IS_SHUTDOWN)))
		return;

	for (int i = 0; i < PandoraRelationGlobalMetadata->metaNums; i++)
	{
		meta = MetaIndexGetPandoraRelationMeta(i);

		/* Wait for remained partitioning workers */
		WaitForPartitioningWorkers(meta);

		/*
		 * Since this is executed after all partitioning workers have finished,
		 * there is no need for concurrency control in the relation metadata.
		 */

		relationOid = meta->tag.relationOid;
		level0RecordCount = meta->recordCountsLevelZero[0] +
							meta->recordCountsLevelZero[1];

		/* If no records exist at level 0, skip */
		if (level0RecordCount == 0)
			continue;

		desc.meta = meta;

		/* Set repartitioning range, including newest mempartition */
		minPartitionNumber = meta->partitionNumberLevelZero[0];
		maxPartitionNumber = meta->partitionNumberLevelZero[1];

		MemSet(reorganizedRecCnt, 0, sizeof(uint32) * 32);
		MemSet(firstSeqNums, -1, sizeof(PandoraSeqNum) * 32);

		pg_memory_barrier();

		/* Pandora repartitioning main entry. */
		PandoraRepartitioning(&desc, relationOid,
							  level0RecordCount,
							  0, 
							  minPartitionNumber, 
							  maxPartitionNumber,
							  1,
							  reorganizedRecCnt,
							  firstSeqNums,
							  false);

		pg_memory_barrier();

		/* level 0 */
		meta->partitionNumberLevelZero[0] =
		++(meta->partitionNumberLevelZero[1]);
		meta->recordCountsLevelZero[0] =
		meta->recordCountsLevelZero[1] = 0;
		meta->recordCountsLevelZero[2] += level0RecordCount;

		/* level 1 */
		for (int i = 0; i < meta->spreadFactor; i++)
		{
			/*
			 * If there are new records that have been repartitioned, apply that
			 * information to the relation metadata.
			 */
			if (reorganizedRecCnt[i] != 0)
			{
				/*
				 * If these repartitioned records are the first of this
				 * partition
				 */
				if (meta->realRecordsCount[1][i] == 0)
				{
					if (likely(meta->lastPartitionLevel != 1))
					{
						/* Increment generation number if needed */
						if (unlikely(++(meta->generationNums[1][i]) == InvalidPandoraPartGenNo))
							meta->generationNums[1][i] = 0;

						/* Set first sequence number of partition */
						meta->locationInfo[1][i].firstSequenceNumber = firstSeqNums[i];
					}
				}

				/* Adds memoized reorganized records count */
				meta->realRecordsCount[1][i] += reorganizedRecCnt[i];
			}
		}

		/* Increment block count for analyze */
		IncrBlockCount(meta, desc.childBlocks - desc.parentBlocks);

		pg_memory_barrier();

		/* GC the repartitioned old file immediately */
		for (PandoraPartNumber partNum = minPartitionNumber;
			 partNum <= maxPartitionNumber;
			 ++partNum)
		{
			snprintf(oldFileName, 32, "base/%u/%u.%u.%u.%u",
					 meta->relation.rd_node.dbNode, relationOid, 0, partNum, 0);
			unlink(oldFileName);
		}
	}
}

#endif /* PANDORA */
