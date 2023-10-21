/*-------------------------------------------------------------------------
 *
 * pandora_relation_meta.c
 *
 * Pandora Relation Meta Implementation
 *
 * Copyright (C) 2021 Scalable Computing Systems Lab.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * IDENTIFICATION
 *    src/backend/pandora/partitioning/pandora_relation_meta.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "miscadmin.h"
#include "common/file_perm.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "access/table.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"
#include "utils/rel.h"
#include "utils/resowner_private.h"
#include "utils/relcache.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"

#ifdef PANDORA
#include "pandora/pandora.h"
#include "pandora/pandora_relation_meta.h"
#include "pandora/pandora_catalog.h"
#include "pandora/pandora_mempart_buf.h"

#define MAX_RELATION_META (32)

/* Relation meta structures. */
PandoraRelationMetaPadded *PandoraRelationMetas;
PandoraRelationGlobalMeta *PandoraRelationGlobalMetadata;

/* src/include/pandora/pandora_relation_meta.h */
dsa_area* relation_meta_dsa_area;

/*
 * PandoraRelationMetaShmemSize 
 *
 * This function returns share memoey size for pandora relation meta. 
 */
Size
PandoraRelationMetaShmemSize(void)
{
	Size size = 0;

	/* pandora relation meta */
	size = add_size(size, mul_size(MAX_RELATION_META, 
									sizeof(PandoraRelationMetaPadded)));

	/* To allow aligning meta descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* pandora relation meta hash */
	size = add_size(size,
		PandoraRelationMetaHashShmemSize(MAX_RELATION_META + 
										NUM_PANDORA_RELATION_META_PARTITIONS));

	/* pandora relation global meta */
	size = add_size(size, sizeof(PandoraRelationGlobalMeta));

	return size;
}


/*
 * PandoraRelationMetaInit
 *
 * Pandora initializes the relation metadata structure at the start of the 
 * database.
 */
void
PandoraRelationMetaInit(void)
{
	bool foundMetas, foundGlobalMeta;
	PandoraRelationMeta *meta;
	char relation_meta_dir[32];

	/* Align descriptors to a cacheline boundary */
	PandoraRelationMetas = (PandoraRelationMetaPadded *) ShmemInitStruct(
			"pandora relation meta datas",
			MAX_RELATION_META * sizeof(PandoraRelationMetaPadded),
			&foundMetas);

	PandoraRelationMetaHashInit(MAX_RELATION_META + 
									NUM_PANDORA_RELATION_META_PARTITIONS);

	/* Initialize metas */
	if (!foundMetas)
	{
		for (int i = 0; i < MAX_RELATION_META; i++)
		{
			meta = MetaIndexGetPandoraRelationMeta(i);

			/* Init values. */
			meta->tag.relationOid = 0;
			meta->metaId = i;
			meta->inited = false;
			meta->partitioningLevel = InvalidPandoraPartLevel;
			meta->partitioningMinNumber = InvalidPandoraPartNumber;
			meta->partitioningMaxNumber = InvalidPandoraPartNumber;
		}
	}

	PandoraRelationGlobalMetadata = (PandoraRelationGlobalMeta *) 
										ShmemInitStruct(
											"pandora relation global meta", 
											sizeof(PandoraRelationGlobalMeta), 
											&foundGlobalMeta);

	/* Initialize global meta */
	if (!foundGlobalMeta)
	{
		PandoraRelationGlobalMetadata->metaNums = 0;
		LWLockInitialize(
			(LWLock*)(&PandoraRelationGlobalMetadata->globalMetaLock),
			LWTRANCHE_PANDORA_PARTITION);
	}

	/* Create relation_meta directory */
	snprintf(relation_meta_dir, 32, "relation_meta");
	if (pg_mkdir_p(relation_meta_dir, pg_dir_create_mode) != 0 &&
		errno != EEXIST)
		elog(ERROR, "Error creating relation_meta directory");
}


/*
 * PandoraGetRelationMeta
 *
 * Pandora maintains metadata per relation for utilization in the buffer cache. 
 * If metadata has been created prior to calling this function, the caller 
 * receives the metadata of the relation. 
 */
PandoraRelationMeta *
PandoraGetRelationMeta(Oid relationOid)
{
	PandoraRelationMeta *meta;
	PandoraRelationMetaTag tag;		/* identity of requested relation */
	LWLock *partitionLock;			/* partition lock for target */
	uint32 hashcode;
	int metaId;

	if (unlikely(relationOid == InvalidOid))
		return NULL;

	tag.relationOid = relationOid;

	/* Calcultae hash code */
	hashcode = PandoraRelationMetaHashCode(&tag);
	partitionLock = PandoraRelationMetaMappingPartitionLock(hashcode);

	LWLockAcquire(partitionLock, LW_SHARED);
	metaId = PandoraRelationMetaHashLookup(&tag, hashcode);
	if (metaId >= 0)
	{
		LWLockRelease(partitionLock);

		meta = MetaIndexGetPandoraRelationMeta(metaId);
		Assert(meta->tag.relationOid == relationOid);
		return meta;
	}

	LWLockRelease(partitionLock);

	/* There is no hash table entry yet, create one */
	if (IsPandoraRelId(relationOid))
		return PandoraCreateRelationMeta(relationOid);

	/* The given relation does not use PANDORA */
	return NULL;
}

/*
 * PandoraPopualateRelationMeta
 *
 * We read the relation metadata file and set pointers to reference it. If the 
 * relation metadata file doesn't exist, the relation metadata is initialized 
 * with zero values.
 */
static void
PandoraPopulateRelationMeta(PandoraRelationMeta *meta)
{
	Oid relationOid = meta->tag.relationOid;
	int64_t currentOffset = 0;
	Relation relation;
	int level;
	bool needInit;

	/* For logging */
	uint32 mega;
	uint32 kilo;
	uint32 bytes;

	Assert(meta->inited == false);

	meta->spreadFactor = PandoraGetSpreadFactor(relationOid);
	meta->lastPartitionLevel = PandoraGetLevelCount(relationOid) - 1;
	pg_atomic_init_u32(&(meta->partitioningWorkerCounter), BlockCheckpointerBit);

	relation = table_open(relationOid, AccessShareLock);

	/* For checkpointer */
	meta->smgr.smgr_rnode.node = RelationGetSmgr(relation)->smgr_rnode.node;
	meta->smgr.smgr_rnode.backend = InvalidBackendId;

	meta->tupDesc = *(relation->rd_att);

	meta->relation.is_siro = relation->is_siro;
	meta->relation.rd_pandora_partitioning = relation->rd_pandora_partitioning;
	meta->relation.rd_node = relation->rd_node;

	meta->relation.rd_smgr = &(meta->smgr);
	meta->relation.rd_att = &(meta->tupDesc);

	/* Check existing relation meta file */
	if (PandoraRelationMetaFileExists(relationOid))
	{
		/* Stored file exists. */
		PandoraReadRelationMetaFile(relationOid, meta->block);	
		needInit = false;
	}
	else
	{
		/* Stored file doesn't exists. */
		memset(meta->block, 0, RELATION_META_BLOCKSIZE);
		needInit = true;

		/* Create new relation meta file and fill zeros. */
		PandoraCreateRelationMetaFile(relationOid);
	}

	/* Set sequence number counter pointer. */
	meta->sequenceNumberCounter = 
							(pg_atomic_uint64 *) (meta->block + currentOffset);
	currentOffset += (int) (sizeof(pg_atomic_uint64) * 1);

	/* Set block counter pointer. */
	meta->nblocks = (pg_atomic_uint64 *) (meta->block + currentOffset);
	currentOffset += (int) (sizeof(pg_atomic_uint64) * 1);

	/* Set last repartitioning's max partition number. */
	meta->partitionNumberLevelZero = 
							(PandoraPartNumber *) (meta->block + currentOffset);
	currentOffset += (int) (sizeof(PandoraPartNumber) * 2);

	/* Set level 0's pointer to stored block. */
	meta->recordCountsLevelZero = (uint64_t *) (meta->block + currentOffset);
	currentOffset += (int) (sizeof(uint64_t) * 3);

	/* Set generation number's pointer to stored block. */
	for (level = 0; level <= meta->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		if (level == 0 || level == meta->lastPartitionLevel)
		{
			/* We don't maintain last levels. */
 			currentPartitionNums = 0;

			meta->generationNums[level] = NULL;
		}
		else
		{
 			currentPartitionNums = (int) pow(meta->spreadFactor, level);

			meta->generationNums[level] = 
						(PandoraPartGenNo *) (meta->block + currentOffset);

			if (needInit)
			{
				memset(meta->generationNums[level], -1, 
							sizeof(PandoraPartGenNo) * currentPartitionNums);
			}
		}

		currentOffset += (int) (sizeof(PandoraPartGenNo) * currentPartitionNums);
	}

	/* Set sequence number's pointer to stored block. */
	for (level = 0; level <= meta->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		if (level == 0 || level == meta->lastPartitionLevel)
		{
			/* We don't maintain last levels. */
 			currentPartitionNums = 0;

			meta->locationInfo[level] = NULL;
		}
		else
		{
 			currentPartitionNums = (int) pow(meta->spreadFactor, level);

			meta->locationInfo[level] = 
						(LocationInfo *) (meta->block + currentOffset);

			if (needInit)
			{
				for (int part = 0; part < currentPartitionNums; ++part)
				{
					meta->locationInfo[level][part].firstSequenceNumber = 0xFFFFFFFFFF;
					meta->locationInfo[level][part].reorganizedRecordsCount = 0;
				}
			}
		}

		currentOffset += (int) (sizeof(uint64_t) * currentPartitionNums);
	}

	/* Set real record count's pointer to stored block. */
	for (level = 0; level <= meta->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		if (unlikely(level == 0))
		{
			/* We don't maintain first levels. */
 			currentPartitionNums = 0;

			meta->realRecordsCount[level] = NULL;
		}
		else
		{
 			currentPartitionNums = (int) pow(meta->spreadFactor, level);

			meta->realRecordsCount[level] = 
						(uint64_t *) (meta->block + currentOffset);
		}

		currentOffset += (int) (sizeof(uint64_t) * currentPartitionNums);
	}

	/* Set cumulative record count's pointer to stored block. */
	{
		int currentPartitionNums;

		/* Cumulative count doesn't be used at level 0 */
		meta->internalCumulativeCount[0] = NULL;

		/*
		 * Cumulative counts of internal levels allow wrap-around, so the size
		 * of variable can be 4-bytes.
		 */
		for (level = 1; level < meta->lastPartitionLevel; ++level)
		{
			currentPartitionNums = (int) pow(meta->spreadFactor, level);

			meta->internalCumulativeCount[level] = 
						(PandoraInPartCnt *) (meta->block + currentOffset);

			currentOffset += (int) (sizeof(PandoraInPartCnt) * currentPartitionNums);
		}

		currentPartitionNums = (int) pow(meta->spreadFactor, level);

		/*
		 * Cumulative count of last level don't allow wrap-around, so the size
		 * of vatiable must be larger than 4-bytes.
		 */
		meta->lastCumulativeCount =
			(PandoraSeqNum *) (meta->block + currentOffset);

		currentOffset += (int) (sizeof(PandoraSeqNum) * currentPartitionNums);
	}

	mega = currentOffset;
	
	bytes = mega % 1024;
	mega /= 1024;

	kilo = mega % 1024;
	mega /= 1024;

	fprintf(stderr, "relation metadata was populated, rel id: %u, size: %u MiB %u KiB %u B\n",
					relationOid, mega, kilo, bytes);

	Assert(currentOffset <= RELATION_META_BLOCKSIZE);

	/* Init locks */
	LWLockInitialize((LWLock*)(&meta->mempartitionLock), LWTRANCHE_PANDORA_PARTITION);

	/* Set ref counters' pointer. */
	currentOffset = 0;
	for (level = 0; level <= meta->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		currentPartitionNums = (int) pow(meta->spreadFactor, level);

		meta->refcounter[level] = 
			(pg_atomic_uint64 *) (meta->refcounter_space + currentOffset);

		currentOffset += (int) (sizeof(pg_atomic_uint64) * currentPartitionNums);
	}

	mega = currentOffset;
	
	bytes = mega % 1024;
	mega /= 1024;

	kilo = mega % 1024;
	mega /= 1024;

	fprintf(stderr, "refcounter, rel id: %u, size: %u MiB %u KiB %u B\n",
					relationOid, mega, kilo, bytes);

	Assert(currentOffset <= RELATION_META_REFCOUNTERSIZE);

	/* Set partition xmax' pointer. */
	currentOffset = 0;
	for (level = 0; level <= meta->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		currentPartitionNums = (int) pow(meta->spreadFactor, level);

		meta->partitionXmax[level] = 
			(TransactionId *) (meta->partition_xmax_space + currentOffset);

		for (int part = 0; part < currentPartitionNums; ++part)
			meta->partitionXmax[level][part] = FirstNormalTransactionId;

		currentOffset += (int) (sizeof(TransactionId) * currentPartitionNums);
	}

	mega = currentOffset;
	
	bytes = mega % 1024;
	mega /= 1024;

	kilo = mega % 1024;
	mega /= 1024;

	fprintf(stderr, "xmax, rel id: %u, size: %u MiB %u KiB %u B\n",
					relationOid, mega, kilo, bytes);

	Assert(currentOffset <= RELATION_META_PARTITION_XMAXSIZE);

	/* Get values from 'Relation'. */
	meta->recordSize = relation->record_size;
	meta->recordNumsPerBlock = relation->records_per_block;
	table_close(relation, AccessShareLock);

	if (needInit)
		PandoraWriteRelationMetaFile(relationOid, meta->block);

	meta->inited = true;
}

/*
 * PandoraCreateRelationMeta
 * 
 * Pandora creates metadata for relations during the creation or alteration of 
 * tables. While the creation logic does not need to account for concurrency 
 * issues, we employ locks for accessing it.
 */
PandoraRelationMeta *
PandoraCreateRelationMeta(Oid relationOid)
{
	PandoraRelationMeta *meta;
	PandoraRelationMetaTag tag;
	Index candidateMetaId;
	LWLock *partitionLock;			/* partition lock for target */
	uint32 hashcode;

	tag.relationOid = relationOid;

	/* Calcuate hash code */
	hashcode = PandoraRelationMetaHashCode(&tag);
	partitionLock = PandoraRelationMetaMappingPartitionLock(hashcode);

	LWLockAcquire((LWLock*)(&PandoraRelationGlobalMetadata->globalMetaLock), LW_EXCLUSIVE);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	for (int i = 0; i < PandoraRelationGlobalMetadata->metaNums; ++i)
	{
		meta = MetaIndexGetPandoraRelationMeta(i);

		if (meta->tag.relationOid == relationOid)
		{
			/* Exists arleady. */
			LWLockRelease(partitionLock);
			LWLockRelease((LWLock*)(&PandoraRelationGlobalMetadata->globalMetaLock));
	
			return meta;
		}
	}

	/* 
	 * Pick up a empry relation meta for a new allocation 
	 */
	candidateMetaId = PandoraRelationGlobalMetadata->metaNums;

	if (MAX_RELATION_META <= PandoraRelationGlobalMetadata->metaNums)
	{
		LWLockRelease(partitionLock);

		elog(WARNING, 
			"we cannot create a new relation metadata due to limitations.");

		return NULL;
	}

	meta = MetaIndexGetPandoraRelationMeta(candidateMetaId);
	Assert(candidateMetaId == meta->metaId);

	/* Init relation metadata. */
	meta->tag = tag;

	/* 
	 * If an existing file exists, we read the relation metadata. If not, we 
	 * populate new relation meta 
	 */
	PandoraPopulateRelationMeta(meta);

	/* Insert hash element to hashtable. */
	PandoraRelationMetaHashInsert(&tag, hashcode, candidateMetaId);
	PandoraRelationGlobalMetadata->metaNums += 1;

	LWLockRelease(partitionLock);
	LWLockRelease((LWLock*)(&PandoraRelationGlobalMetadata->globalMetaLock));

	return meta;
}

/*
 * PandoraFlushRelationMeta
 *
 * Pandora records the relation metadata block in the metadata file of the 
 * relation.
 */
void
PandoraFlushRelationMeta(PandoraRelationMeta *meta)
{
	LWLock *metaMempartitionLock = PandoraRelationMetaMemPartitionLock(meta);

	if (unlikely(meta->tag.relationOid == InvalidOid))
		return;

	LWLockAcquire(metaMempartitionLock, LW_SHARED);
	PandoraWriteRelationMetaFile(meta->tag.relationOid, meta->block);
	LWLockRelease(metaMempartitionLock);
}

/*
 * PandoraFlushAllRelationMeta
 *
 * Flush all relation metadata currently opened.
 */
void
PandoraFlushAllRelationMeta(void)
{
	for (int i = 0; i < PandoraRelationGlobalMetadata->metaNums; i++)
	{
		PandoraFlushRelationMeta(MetaIndexGetPandoraRelationMeta(i));
	}
}

/*
 * PandoraCheckAllRelationMetaRecordsCount
 * 
 * Print the sequence number and records count of all metas.
 */
void
PandoraCheckAllRelationMetaRecordsCount(void)
{
	PandoraRelationMeta *meta;
	LWLock *metaMempartitionLock;
	int lastLevel;
	int spreadFactor;
	uint16 recordNumsPerBlock;
	uint64 recCntSum;
	uint64 recCnt_lvl0_real;
	uint64 recCnt_lvl0_tiered;
	uint64 recCnt_lvl0_reoganized;
	uint64 recCntLvlSum[32] = { 0 };
	uint64 seqNum;
	uint64 *levelRealRecCnt;
	uint64 nblocks;
	uint64 nblocksLvl[32] = { 0 };
	uint64 tmpCnt;

	pg_atomic_uint64 *refcounter;
	uint32 ref_oldval;

	for (int i = 0; i < PandoraRelationGlobalMetadata->metaNums; i++)
	{
		meta = MetaIndexGetPandoraRelationMeta(i);
		metaMempartitionLock = PandoraRelationMetaMemPartitionLock(meta);
		lastLevel = meta->lastPartitionLevel;
		spreadFactor = meta->spreadFactor;
		recordNumsPerBlock = meta->recordNumsPerBlock;

		for (int tmp = 0; tmp < 32; ++tmp)
		{
			recCntLvlSum[tmp] = nblocksLvl[tmp] = 0;
		}

		fprintf(stderr, "[PANDORA] relation oid: %u\n", meta->tag.relationOid);

		/* Acquire lock, and increment refcounts */
		LWLockAcquire(metaMempartitionLock, LW_SHARED);

		ref_oldval = pg_atomic_read_u64(PandoraRelationMetaRefCounter(meta, 0, 0));

		if (ref_oldval)
			fprintf(stderr, "[PANDORA] level: 0, refcount: 0x%08x\n", ref_oldval);

		for (int level = 1; level <= lastLevel; ++level)
		{
			for (PandoraPartNumber part_num = 0;
				 part_num < (PandoraPartNumber) pow(spreadFactor, level);
				 ++part_num)
			{
				/* Don't use MACRO for log */
				refcounter = PandoraRelationMetaRefCounter(meta, level, part_num);
retry:
				WaitForMetaDataBit(refcounter);
				ref_oldval = pg_atomic_fetch_add_u64(refcounter, MetaDataCount);
				if (unlikely((ref_oldval & MetaDataBit) == 1))
				{
					DecrMetaDataCount(refcounter);
					goto retry;
				}

				if (ref_oldval)
					fprintf(stderr, "[PANDORA] level: %u, partNum: %u, refcount: 0x%08x\n", level, part_num, ref_oldval);

				// IncrMetaDataCount(PandoraRelationMetaRefCounter(meta, level, part_num));
			}
		}
		
		seqNum = pg_atomic_read_u64(meta->sequenceNumberCounter);
		nblocks = pg_atomic_read_u64(meta->nblocks);

		recCnt_lvl0_real = meta->recordCountsLevelZero[0];
		recCnt_lvl0_tiered = meta->recordCountsLevelZero[1];
		recCnt_lvl0_reoganized = meta->recordCountsLevelZero[2];
		recCntSum = recCnt_lvl0_real + recCnt_lvl0_tiered;
		recCntLvlSum[0] = recCntSum;

		if (recCntSum != 0)
			nblocksLvl[0] = NBlocksUsingRecordCount(recCntSum, recordNumsPerBlock);
		
		for (int level = 1; level <= lastLevel; ++level)
		{
			levelRealRecCnt = meta->realRecordsCount[level];

			for (PandoraPartNumber part_num = 0;
				 part_num < (PandoraPartNumber) pow(spreadFactor, level);
				 ++part_num)
			{
				tmpCnt = levelRealRecCnt[part_num];
				recCntSum += tmpCnt;
				recCntLvlSum[level] += tmpCnt;

				if (tmpCnt != 0)
					nblocksLvl[level] += NBlocksUsingRecordCount(tmpCnt, recordNumsPerBlock);
			}
		}

		/* Release lock, and decrement refcounts */
		for (int level = lastLevel; level > 0; --level)
		{
			for (PandoraPartNumber part_num = 0;
				 part_num < (PandoraPartNumber) pow(spreadFactor, level);
				 ++part_num)
			{
				DecrMetaDataCount(PandoraRelationMetaRefCounter(meta, level, part_num));
			}
		}
		LWLockRelease(metaMempartitionLock);

		fprintf(stderr, "[PANDORA] seqNum: %lu, sum: %lu (nblocks: %lu)\n", seqNum, recCntSum, nblocks);
		fprintf(stderr, "[PANDORA] level 0, real: %lu, tiered: %lu, reorganized: %lu, sum: %lu\n", recCnt_lvl0_real, recCnt_lvl0_tiered, recCnt_lvl0_reoganized, recCnt_lvl0_real+recCnt_lvl0_tiered+recCnt_lvl0_reoganized);
		for (int level = 0; level <= lastLevel; ++level)
			fprintf(stderr, "[PANDORA] level: %d, sum: %lu (nblocks: %lu)\n", level, recCntLvlSum[level], nblocksLvl[level]);
	}
}

/*
 * PandoraCheckRelationMetaRecordsCount
 * 
 * Print the sequence number and records count of one meta.
 * Caller must acquires lock and increment all refcounts before calling this.
 */
void
PandoraCheckRelationMetaRecordsCount(PandoraRelationMeta *meta)
{
	int lastLevel;
	uint64 recCntSum;
	uint64 recCnt_lvl0_real;
	uint64 recCnt_lvl0_tiered;
	uint64 recCnt_lvl0_reoganized;
	uint64 recCntLvlSum[32] = { 0 };
	uint64 seqNum;
	uint64 *levelRealRecCnt;
	uint64 nblocks;

	lastLevel = meta->lastPartitionLevel;
	
	seqNum = pg_atomic_read_u64(meta->sequenceNumberCounter);
	nblocks = pg_atomic_read_u64(meta->nblocks);

	recCnt_lvl0_real = meta->recordCountsLevelZero[0];
	recCnt_lvl0_tiered = meta->recordCountsLevelZero[1];
	recCnt_lvl0_reoganized = meta->recordCountsLevelZero[2];
	recCntSum = recCnt_lvl0_real + recCnt_lvl0_tiered;
	recCntLvlSum[0] = recCntSum;	
	
	for (int level = 1; level <= lastLevel; ++level)
	{
		levelRealRecCnt = meta->realRecordsCount[level];

		for (PandoraPartNumber part_num = 0;
				part_num < (PandoraPartNumber) pow(meta->spreadFactor, level);
				++part_num)
		{
			recCntSum += levelRealRecCnt[part_num];
			recCntLvlSum[level] += levelRealRecCnt[part_num];
		}
	}

	fprintf(stderr, "[PANDORA] relation oid: %u\n", meta->tag.relationOid);
	fprintf(stderr, "[PANDORA] seqNum: %lu, sum: %lu (nblocks: %lu)\n", seqNum, recCntSum, nblocks);
	fprintf(stderr, "[PANDORA] level 0, real: %lu, tiered: %lu, reorganized: %lu, sum: %lu\n", recCnt_lvl0_real, recCnt_lvl0_tiered, recCnt_lvl0_reoganized, recCnt_lvl0_real+recCnt_lvl0_tiered+recCnt_lvl0_reoganized);
	for (int level = 0; level <= lastLevel; ++level)
			fprintf(stderr, "[PANDORA] level: %d, sum: %lu\n", level, recCntLvlSum[level]);
}

/*
 * SetRepartitioningStatusInprogress
 */
void
SetRepartitioningStatusInprogress(pg_atomic_uint64 *refcounter)
{
	uint64 oldValue = 
		pg_atomic_fetch_or_u64(refcounter, REPARTITIONING_INPROGRESS);

	/* This partition should not be in repartitioning. */
	Assert((oldValue & REPARTITIONING_FLAG_MASK) == 0);

	/* Wait normal modifications. */
	WaitForNormalModificationDone(refcounter);
}

/*
 * SetRepartitioningStatusModificationBlocking
 */
void
SetRepartitioningStatusModificationBlocking(pg_atomic_uint64 *refcounter)
{
	uint64 oldValue = 
		pg_atomic_fetch_or_u64(refcounter, REPARTITIONING_MODIFICATION_BLOCKING);

	/* This partition should already have the repartitioning inprogress flag. */
	Assert((oldValue & REPARTITIONING_FLAG_MASK) == REPARTITIONING_INPROGRESS);
	Assert(GetNormalModificationCount(refcounter) == 0);	

	/* Wait modifications with delta. */
	WaitForModificationWithDeltaDone(refcounter);
}

/*
 * SetRepartitioningStatusDone
 */
void
SetRepartitioningStatusDone(pg_atomic_uint64 *refcounter)
{
	uint64 oldValue;

	Assert(GetModificationWithDeltaCount(refcounter) == 0);	

	/* Unset all repartitioning status flags. */
	oldValue = pg_atomic_fetch_and_u64(refcounter, 
					~(REPARTITIONING_INPROGRESS | 
						REPARTITIONING_MODIFICATION_BLOCKING |
						MetaDataBit));

	/* 
	 * This partition should already have the repartitioning inprogress and 
	 * blocking flags. 
	 */
	Assert((oldValue & REPARTITIONING_FLAG_MASK) == 
		(REPARTITIONING_INPROGRESS | REPARTITIONING_MODIFICATION_BLOCKING));
}

/*
 * GetModificationMethod
 * 
 * Modification workers need to get their method to modify tuples. So, this 
 * function gets target partition's refcounter and returns appropriate 
 * modification method.
 */
PandoraModificationMethod
GetModificationMethod(PandoraRelationMeta *meta, pg_atomic_uint64 *refcounter,
									PandoraPartLevel targetPartitionLevel, 
									PandoraPartNumber targetPartitionNumber)
{
	uint64 refcntForChecking = 0;
	uint64 refcnt;

	/* The caller increments meta data counter. */
	Assert(GetMetaDataCount(refcounter) > 0);

retry_get_modification_method:
	/* Read reference counter. */
	refcnt = pg_atomic_read_u64(refcounter);
	pg_memory_barrier();

	if ((refcnt & (REPARTITIONING_INPROGRESS | 
					REPARTITIONING_MODIFICATION_BLOCKING)) == 0ULL) 
	{
		/* 
		 * Repartitioning is not inprogress.
		 */
		
		/* Increment normal modification count. */
		refcntForChecking = IncrNormalModificationCount(refcounter);

		if ((refcntForChecking & (REPARTITIONING_INPROGRESS |
							REPARTITIONING_MODIFICATION_BLOCKING)) != 0ULL)
		{
			/* Repartitioning worker intervene here. Just retry it. */
			DecrNormalModificationCount(refcounter);
			goto retry_get_modification_method;
		}

		return NORMAL_MODIFICATION;
	}
	else if (refcnt & REPARTITIONING_MODIFICATION_BLOCKING)
	{
		/* 
		 * The worker cannot proceed with modification for the partition.
		 * Caller needs to go downside partition to find the tuple.
		 */
		Assert(refcnt & REPARTITIONING_INPROGRESS);

		/*
		 * corner case 
		 * the repartitioning worker does not perform repartitioning on the 
		 * update worker's target.
		 */
		if (CheckNonPartitioningInLevelZero(meta, 
					targetPartitionLevel, targetPartitionNumber) &&
					(refcntForChecking & REPARTITIONING_MODIFICATION_BLOCKING))
		{
			goto retry_get_modification_method;
		}

		return MODIFICATION_BLOCKING;
	}
	else
	{
		/* 
		 * Repartitioning is inprogress. But, the worker can do modification 
		 * with delta. 
		 */
		Assert(refcnt & REPARTITIONING_INPROGRESS);

		/* Increment normal modification count. */
		refcntForChecking = IncrModificationWithDeltaCount(refcounter);
		Assert(refcntForChecking & REPARTITIONING_INPROGRESS);

		if (refcntForChecking & REPARTITIONING_MODIFICATION_BLOCKING)
		{
			/* 
			 * At this point, the repartitioning worker intervenes. If 
			 * repartitioning enters the blocking phase, we won't be able to 
			 * find the tuple in this partition. 
			 */
			DecrModificationWithDeltaCount(refcounter);
	
			/*
			 * corner case 
			 * the repartitioning worker does not perform repartitioning on the 
			 * update worker's target.
			 */
			if (CheckNonPartitioningInLevelZero(meta, 
					targetPartitionLevel, targetPartitionNumber) &&
					(refcntForChecking & REPARTITIONING_MODIFICATION_BLOCKING))
			{
				goto retry_get_modification_method;
			}

			return MODIFICATION_BLOCKING;
		}

		return MODIFICATION_WITH_DELTA;
	}

	/* quiet compiler */
	Assert(false);
	return MODIFICATION_BLOCKING;
}

/*
 * DecrModificationCount
 */
void
DecrModificationCount(pg_atomic_uint64 *refcounter, 
				PandoraModificationMethod modificationMethod)
{
	/* 
	 * If modification method is not 'normal' or 'with delta', that is an 
	 * error.
	 */
	Assert((modificationMethod == NORMAL_MODIFICATION) || 
				(modificationMethod == MODIFICATION_WITH_DELTA));

	if (modificationMethod == NORMAL_MODIFICATION)
		DecrNormalModificationCount(refcounter);
	else if (modificationMethod == MODIFICATION_WITH_DELTA)
		DecrModificationWithDeltaCount(refcounter);
}

/*
 * UnsetAllBlockCheckpointerBits
 * 
 * Unset BlockCheckpointerBits of all relation metadata to notify checkpointer
 * that repartitioning newest mempartition is safe from now.
 */
void
UnsetAllBlockCheckpointerBits(void)
{
	PandoraRelationMeta *meta;

	for (int i = 0; i < PandoraRelationGlobalMetadata->metaNums; i++)
	{
		meta = MetaIndexGetPandoraRelationMeta(i);

		if (IsNoWorker(meta))
			UnsetBlockCheckpointerBit(meta);
	}
}

/*
 * Append level 0 stat into the given string. There is no partition metadata for
 * each tiers. But each of them has same size. So estimate the page count using
 * that knowledge.
 */
static inline void
pandora_append_level_0_stat(PandoraRelationMeta *meta, StringInfo line)
{
	appendStringInfo(line, "%u ",
		(meta->partitionNumberLevelZero[1] - meta->partitionNumberLevelZero[0])
			* PANDORA_MEMPART_PAGE_COUNT);
}

/*
 * Append non-zero level stat into the given string.
 */
static inline void
pandora_append_non_0_level_stat(PandoraRelationMeta *meta,
								Relation rel, StringInfo line)
{
	for (PandoraPartLevel level = 1;
		 level <= meta->lastPartitionLevel; level++)
	{
		PandoraPartNumber max_partnum = pow(rel->rd_pandora_spread_factor,
											level);
		uint64_t page_cnt = 0;

		for (PandoraPartNumber partnum = 0;
			 partnum < max_partnum; partnum++)
		{
			uint64_t recordcnt = meta->realRecordsCount[level][partnum];
	
			Assert(meta->recordNumsPerBlock != 0);
			page_cnt += recordcnt % meta->recordNumsPerBlock == 0 ?
						recordcnt / meta->recordNumsPerBlock :
						recordcnt / meta->recordNumsPerBlock + 1;
		}
	
		appendStringInfo(line, "%lu ", page_cnt);
	}
}

/*
 * Write level stat string into the specified directory.
 */
static inline void
pandora_write_level_stat_str(PandoraRelationMeta *meta,
							 Oid relid, char *dir)
{
	Relation rel = table_open(relid, AccessShareLock);
	struct timespec curr;
	StringInfo line;
	char path[256];
	FILE *fp;

	/* Initialize line */
	line = palloc0(sizeof(StringInfoData));
	initStringInfo(line);

	/* First column is current time (second) */
	clock_gettime(CLOCK_MONOTONIC, &curr);
	appendStringInfo(line, "%lu ", curr.tv_sec);

	/* Append level 0's stat */
	pandora_append_level_0_stat(meta, line);

	/* Append other level's stats */
	pandora_append_non_0_level_stat(meta, rel, line);

	/* Write the line */
	sprintf(path, "%s/%s", dir, RelationGetRelationName(rel));
	fp = fopen(path, "a");
	fprintf(fp, "%s\n", line->data);
	fclose(fp);

	/* Done */
	pfree(line);
	table_close(rel, AccessShareLock);
}

/*
 * Function for stat.
 */
Datum
pandora_get_level_stat(PG_FUNCTION_ARGS)
{
	char *dir = text_to_cstring(PG_GETARG_TEXT_P(0));
	List *relid_list = PandoraGetRelOidList();
	ListCell *lc;

	/* Make a given directory automatically */
	if (mkdir(dir, 0777) == -1 && errno != EEXIST)
		elog(ERROR, "mkdir error");

	/* Write level stat string for pandora tables */
	foreach(lc, relid_list)
	{
		Oid relid = lfirst_oid(lc);
		PandoraRelationMeta *meta = PandoraGetRelationMeta(relid);

		if (meta == NULL)
			continue;

		pandora_write_level_stat_str(meta, relid, dir);
	}

	PG_RETURN_VOID();
}
#else /* !PANDORA */
/*
 * Dummy function
 */
Datum
pandora_get_level_stat(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}
#endif /* PANDORA */
