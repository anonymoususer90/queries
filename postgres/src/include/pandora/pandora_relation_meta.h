/*-------------------------------------------------------------------------
 *
 * pandora_relation_meta.h
 *    Pandora Relation Meta
 *
 *
 * src/include/pandora/pandora_relation_meta.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PANDORA_RELATION_META_H
#define PANDORA_RELATION_META_H

#include "utils/dsa.h"
#include "utils/rel.h"
#include "port/atomics.h"
#include "storage/s_lock.h"
#include "storage/lwlock.h"
#include "pandora/pandora.h"

#ifdef WAIT_YIELD
#include <pthread.h>
#endif /* WAIT_YIELD */

typedef struct
{
	Oid relationOid; 	/* target relation oid */
} PandoraRelationMetaTag;

typedef struct
{
	PandoraRelationMetaTag tag;
	int id;				/* index */
} PandoraRelationMetaLookupEntry;

#define PandoraRelationMetaHashPartition(hashcode) \
							((hashcode) % NUM_PANDORA_RELATION_META_PARTITIONS)
#define PandoraRelationMetaMappingPartitionLock(hashcode)          \
	(&MainLWLockArray[PANDORA_RELATION_META_MAPPING_LWLOCK_OFFSET + \
					  PandoraRelationMetaHashPartition(hashcode)]  \
		  				.lock)
#define PandoraRelationMetaMemPartitionLock(meta) \
	((LWLock*)(&((meta)->mempartitionLock)))
#define PandoraRelationMetaReorganizationLock(meta) \
	((LWLock*)(&((meta)->reorganizationLock)))
#define PandoraRelationMetaRefCounter(meta, level, partNum) \
	(&((meta)->refcounter[level][partNum]))

#define RELATION_META_BLOCKSIZE (1024 * 1024 * 32) /* 32 MiB */
#define RELATION_META_REFCOUNTERSIZE (1024 * 1024 * 16) /* 16 MiB */
#define RELATION_META_PARTITION_XMAXSIZE (1024 * 1024 * 8) /* 8 MiB */

#define IncrBlockCount(meta, n) \
	(pg_atomic_fetch_add_u64((meta)->nblocks, (n)))
#define PandoraGetNumRows(rel) \
	(pg_atomic_read_u64((PandoraGetRelationMeta(RelationGetRelid(rel)))->sequenceNumberCounter))
#define PandoraGetNBlocks(rel) \
	(pg_atomic_read_u64((PandoraGetRelationMeta(RelationGetRelid(rel)))->nblocks))

/*
 * reference counter of relation metadata
 * (for avoiding conflict with cleaning worker) 
 */
#define BlockCheckpointerBit						(0x80000000)
#define UnsetBlockCheckpointerBit(meta) \
	(pg_atomic_fetch_and_u32(&(meta->partitioningWorkerCounter), ~BlockCheckpointerBit))

#define IncrPartitioningWorkerCount(meta) \
	(pg_atomic_fetch_add_u32(&(meta->partitioningWorkerCounter), 1))
#define DecrPartitioningWorkerCount(meta) \
	(pg_atomic_fetch_sub_u32(&(meta->partitioningWorkerCounter), 1))
#define IsNoWorker(meta) \
	((pg_atomic_read_u32(&(meta->partitioningWorkerCounter)) & ~BlockCheckpointerBit) == 0)

/*
 * reference counter of partitions:
 * 
 * ---------------------------------------------------------------------
 * |     2-bits     |   15-bits    |    15-bits   | 1-bit  |  15-bits  |
 * |      flag      |  reference   |   reference  | toggle | reference |
 * |      bits      |    count     |     count    |  bit   |   count   |
 * ---------------------------------------------------------------------
 * | repartitioning |    normal    | modification |     meta data      |
 * |     status     | modification |   w/ delta   |                    |
 * ---------------------------------------------------------------------
 */

#define MetaDataBit									(0x0000000000008000ULL)
#define MetaDataCount								(0x0000000000000001ULL)
#define MetaDataMask								(0x0000000000007FFFULL)
#define MODIFICATION_WITH_DELTA_COUNT_ONE			(1ULL << 16)
#define MODIFICATION_WITH_DELTA_COUNT_MASK			(0x7FFFULL << 16)
#define NORMAL_MODIFICATION_COUNT_ONE				(1ULL << 31)
#define NORMAL_MODIFICATION_COUNT_MASK				(0x7FFFULL << 31)
#define REPARTITIONING_FLAG_MASK					(0x3ULL << 46)

/* repartitioning status flag */
#define REPARTITIONING_MODIFICATION_BLOCKING		(1ULL << 46)
#define REPARTITIONING_INPROGRESS					(1ULL << 47)

#define SetMetaDataBitInternal(refcounter) \
	(pg_atomic_fetch_or_u64(refcounter, MetaDataBit))
#define UnsetMetaDataBit(refcounter) \
	(pg_atomic_fetch_and_u64(refcounter, ~MetaDataBit))
#define GetMetaDataBit(refcounter) \
	(pg_atomic_read_u64(refcounter) & MetaDataBit)

#define IncrMetaDataCountInternal(refcounter) \
	(pg_atomic_fetch_add_u64(refcounter, MetaDataCount) & MetaDataBit)
#define DecrMetaDataCount(refcounter) \
	(pg_atomic_fetch_sub_u64(refcounter, MetaDataCount))
#define GetMetaDataCount(refcounter) \
	(pg_atomic_read_u64(refcounter) & MetaDataMask)

#define GetNormalModificationCount(refcounter) \
	((pg_atomic_read_u64(refcounter) & NORMAL_MODIFICATION_COUNT_MASK) >> 31)
#define IncrNormalModificationCount(refcounter) \
	(pg_atomic_fetch_add_u64(refcounter, NORMAL_MODIFICATION_COUNT_ONE))
#define DecrNormalModificationCount(refcounter) \
	(pg_atomic_fetch_sub_u64(refcounter, NORMAL_MODIFICATION_COUNT_ONE))

#define GetModificationWithDeltaCount(refcounter) \
	((pg_atomic_read_u64(refcounter) & MODIFICATION_WITH_DELTA_COUNT_MASK) >> 16)
#define IncrModificationWithDeltaCount(refcounter) \
	(pg_atomic_fetch_add_u64(refcounter, MODIFICATION_WITH_DELTA_COUNT_ONE))
#define DecrModificationWithDeltaCount(refcounter) \
	(pg_atomic_fetch_sub_u64(refcounter, MODIFICATION_WITH_DELTA_COUNT_ONE))

#define CheckNonPartitioningInLevelZero(relationMeta, targetPartitionLevel, targetPartitionNumber) \
	(targetPartitionLevel == 0 && \
		relationMeta->partitioningLevel == 0 && \
		relationMeta->partitioningMaxNumber < targetPartitionNumber)

#pragma GCC push_options
#pragma GCC optimize ("O0")

#ifdef WAIT_YIELD
#define WaitForMetaDataBit(refcounter) \
	do { \
		while (GetMetaDataBit(refcounter) != 0) \
			{pthread_yield();} \
	} while (0)

#define WaitForMetaDataCount(refcounter) \
	do { \
		while (GetMetaDataCount(refcounter) != 0) \
			{pthread_yield();} \
	} while (0)

#define WaitForNormalModificationDone(refcounter) \
	do { \
		while (GetNormalModificationCount(refcounter) != 0) \
		{pthread_yield();} \
	} while (0)

#define WaitForModificationWithDeltaDone(refcounter) \
	do { \
		while (GetModificationWithDeltaCount(refcounter) != 0) \
		{pthread_yield();} \
	} while (0)

#define WaitForPartitioningWorkers(meta) \
	do { \
		while (pg_atomic_read_u32(&(meta->partitioningWorkerCounter)) != 0) \
			{pthread_yield();} \
	} while (0)

#else /* !WAIT_YIELD */

#define WaitForMetaDataBit(refcounter) \
	do { \
		while (GetMetaDataBit(refcounter) != 0) \
			{SPIN_DELAY();} \
	} while (0)

#define WaitForMetaDataCount(refcounter) \
	do { \
		while (GetMetaDataCount(refcounter) != 0) \
			{SPIN_DELAY();} \
	} while (0)

#define WaitForNormalModificationDone(refcounter) \
	do { \
		while (GetNormalModificationCount(refcounter) != 0) \
		{SPIN_DELAY();} \
	} while (0)

#define WaitForModificationWithDeltaDone(refcounter) \
	do { \
		while (GetModificationWithDeltaCount(refcounter) != 0) \
		{SPIN_DELAY();} \
	} while (0)

#define WaitForPartitioningWorkers(meta) \
	do { \
		while (pg_atomic_read_u32(&(meta->partitioningWorkerCounter)) != 0) \
			{SPIN_DELAY();} \
	} while (0)

#endif /* WAIT_YIELD */
#pragma GCC pop_options

#define SetMetaDataBit(refcounter) \
	do { \
		if ((SetMetaDataBitInternal(refcounter) & MetaDataMask) != 0) \
			WaitForMetaDataCount(refcounter); \
	} while (0)

#define IncrMetaDataCount(refcounter) \
	do { \
		while (true) { \
			WaitForMetaDataBit(refcounter); \
			if (likely(IncrMetaDataCountInternal(refcounter) == 0)) \
				break; \
			DecrMetaDataCount(refcounter); \
		} \
	} while (0)

/* next optimization */
// typedef union LocationInfo
// {
// 	struct /* This struct is for internal level */
// 	{
// 		uint64 reorganizedRecordsCount: 24,
// 			   firstSequenceNumber: 40;
// 	};

// 	/* This variable is for last level */
// 	PandoraSeqNum lastRecCnt;
// } LocationInfo;

typedef struct LocationInfo
{
	uint64 reorganizedRecordsCount: 24,
		   firstSequenceNumber: 40;
} LocationInfo;

/*
 * PandoraRelationMeta
 */
typedef struct PandoraRelationMeta
{
	Index metaId;				/* identity */
	PandoraRelationMetaTag tag; /* tag for hashtable logic */
	bool inited;				/* is init? */

	/*
	 * These are only for repartitioning of checkpointer.
	 * Checkpointer can't open relation during repartitioning, so used backed up
	 * data.
	 */
	RelationData relation;
	SMgrRelationData smgr;
	TupleDescData tupDesc;

	/*
	 * msb: blocking checkpointer, left bits: worker counter
	 * concurrency control between partitioning worker and checkpointing worker
	 */
	pg_atomic_uint32 partitioningWorkerCounter;
	
	/*
	 * When the value of 'spreadFactor' is two, our perspective is as follows:
	 * 
	 * [Example] (level count: n)
	 *
	 * (level)
	 *    0     ▢▢▢
	 *           ↓--------↘
	 * 	  1     ▢▢▢       ▢▢▢
	 *           ↓---↘     ↓---↘
	 *    2     ▢▢▢  ▢▢▢  ▢▢▢  ▢▢▢
	 *           ↓↘   ↓↘   ↓↘   ↓↘
	 *   ...
	 *           ↓    ↓    ↓    ↓
	 *   n-1    ▢▢▢  ▢▢▢  ▢▢▢  ▢▢▢  ...
	 *
	 * We utilize the first index of generationNum as the partition level and 
	 * the second index as the partition number.						
	 */

	int spreadFactor;							/* fanout number */
	int lastPartitionLevel;						/* higest partition level */

	uint16 recordSize;
	uint16 recordNumsPerBlock;

	PandoraPartLevel partitioningLevel;
	PandoraPartNumber partitioningMinNumber;
	PandoraPartNumber partitioningMaxNumber;

	pg_atomic_uint64 *sequenceNumberCounter;
	pg_atomic_uint64 *nblocks;

	/* 
	 * We have two workers for partitioning, so we control the concurrency 
	 * between them using locks. 
	 */
	LWLockPadded levelOnePartitioningLock[32];

	/*
	 * - protection -
	 * relation meta lvl0 lock:
	 *  - partitionNumberLevelZero[0]
	 *  - partitionNumberLevelZero[1]
	 *  - recordCountsLevelZero[0]
	 *  - recordCountsLevelZero[1]
	 * ------------------------------------
	 * relation meta lvl0 ref counter:
	 *  - recordCountsLevelZero[2]
	 */
	LWLockPadded	mempartitionLock;
	LWLockPadded	reorganizationLock;
	PandoraPartNumber *partitionNumberLevelZero; /* [0]: previous, [1]: current */
	uint64 *recordCountsLevelZero; /* [0]: current, [1]: tierd, [2]: reorganized */

	/* pandora delta space - PandoraDeltaSpaceEntry */
	/* for level 0, 1 */
	volatile dsa_pointer dsaPartitionDeltaSpaceEntryForLevelZeroOne;	
	/* for level 2 and above */
	volatile dsa_pointer dsaPartitionDeltaSpaceEntryForOthers;	

	/* 
	 * The generation number for partition levels 0 and N (highest) is both set 
	 * to 0, so this variable does not reference the first and last levels.
	 * This implies that 'generationNums[0]' and 'generationNums[n]' have NULL 
	 * values.
	 */
	PandoraPartGenNo *generationNums[32];			/* [level][number] */
	uint64 *realRecordsCount[32];					/* [level][number] */
	pg_atomic_uint64 *refcounter[32];				/* [level][number] */
	TransactionId *partitionXmax[32];				/* [level][number] */
	LocationInfo *locationInfo[32];					/* [level][number] */
	PandoraInPartCnt *internalCumulativeCount[32];	/* [internal-level][number] */
	PandoraSeqNum *lastCumulativeCount;				/* [  last-level  ][number] */

	char refcounter_space[RELATION_META_REFCOUNTERSIZE];
	char partition_xmax_space[RELATION_META_PARTITION_XMAXSIZE];
	char block[RELATION_META_BLOCKSIZE];	/* meta block */
} PandoraRelationMeta;

#define PANDORA_RELATION_META_PAD_TO_SIZE \
							(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union PandoraRelationMetaPadded {
	PandoraRelationMeta meta;
	char pad[PANDORA_RELATION_META_PAD_TO_SIZE];
} PandoraRelationMetaPadded;

/* Metadata per pandora's relation meta */
typedef struct PandoraRelationGlobalMeta
{
	dsa_handle handle;		/* dsa handle */
	int metaNums;
	LWLockPadded globalMetaLock;
} PandoraRelationGlobalMeta;

extern PandoraRelationMetaPadded *PandoraRelationMetas;
extern PandoraRelationGlobalMeta *PandoraRelationGlobalMetadata;

/* Macros used as helper functions */
#define MetaIndexGetPandoraRelationMeta(id) \
							(&PandoraRelationMetas[(id)].meta)

/* backend/pandora/partitioning/pandora_relation_meta.c */
extern dsa_area* relation_meta_dsa_area;

/* Public functions */

/* backend/pandora/partitioning/pandora_relation_meta.c */
extern Size PandoraRelationMetaShmemSize(void);
extern void PandoraRelationMetaInit(void);
extern PandoraRelationMeta *PandoraGetRelationMeta(Oid relationOid);
extern PandoraRelationMeta *PandoraCreateRelationMeta(Oid relationOid);
extern void PandoraFlushRelationMeta(PandoraRelationMeta *meta);
extern void SetRepartitioningStatusInprogress(pg_atomic_uint64 *refcounter);
extern void SetRepartitioningStatusModificationBlocking(pg_atomic_uint64 *refcounter);
extern void SetRepartitioningStatusDone(pg_atomic_uint64 *refcounter);
extern PandoraModificationMethod GetModificationMethod(PandoraRelationMeta *meta, 
									pg_atomic_uint64 *refcounter,
									PandoraPartLevel targetPartitionLevel, 
									PandoraPartNumber targetPartitionNumber);
extern void DecrModificationCount(pg_atomic_uint64 *refcounter, 
								PandoraModificationMethod modificationMethod);
extern void PandoraFlushAllRelationMeta(void);

/* backend/pandora/partitioning/pandora_relation_meta_hash.c */
extern Size PandoraRelationMetaHashShmemSize(int size);
extern void PandoraRelationMetaHashInit(int size);
extern uint32 PandoraRelationMetaHashCode(const PandoraRelationMetaTag* tagPtr);
extern int PandoraRelationMetaHashLookup(const PandoraRelationMetaTag* tagPtr, 
															uint32 hashcode);
extern int PandoraRelationMetaHashInsert(const PandoraRelationMetaTag* tagPtr,
												uint32 hashcode, int page_id);
extern void PandoraRelationMetaHashDelete(const PandoraRelationMetaTag* tagPtr, 
															uint32 hashcode);

/* backend/pandora/partitioning/pandora_relation_meta_file.c */
extern void PandoraCreateRelationMetaFile(const Oid relationOid);
extern bool PandoraRelationMetaFileExists(const Oid relationOid);
extern void PandoraReadRelationMetaFile(const Oid relationOid, char *block);
extern void PandoraWriteRelationMetaFile(const Oid relationOid, char *block);

/* backend/pandora/partitioning/pandora_relation_meta_utils.c */
extern int PandoraGetTotalPartitionNums(PandoraRelationMeta *meta, 
												bool includeLastLevel);
extern PandoraPartGenNo PandoraGetPartGenerationNumber(PandoraRelationMeta *meta, 
											PandoraPartLevel partitionLevel, 
											PandoraPartNumber partitionNumber);
extern bool IsPartitionOldFile(Oid relationOid,
							   PandoraPartLevel partitionLevel, 
							   PandoraPartNumber partitionNumber, 
							   PandoraPartGenNo generationNumber);
extern uint64 IncrementSequenceNumber(PandoraRelationMeta *meta);
extern void PandoraCheckAllRelationMetaRecordsCount(void);
extern void PandoraCheckRelationMetaRecordsCount(PandoraRelationMeta *meta);
extern void UnsetAllBlockCheckpointerBits(void);

#endif /* PANDORA_RELATION_META_H */
