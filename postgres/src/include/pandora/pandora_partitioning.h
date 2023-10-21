/*-------------------------------------------------------------------------
 *
 * pandora_partitioning.h
 *	  repartitioning implementation of PANDORA.
 *
 *
 * src/include/pandora/pandora_partitioning.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PANDORA_PARTITIONING_H
#define PANDORA_PARTITIONING_H

#include "utils/dsa.h"
#include "pg_config.h"
#include "utils/relcache.h"
#include "access/htup_details.h"
#include "pandora/pandora.h"
#include "pandora/pandora_relation_meta.h"
#include "postmaster/pandora_partition_mgr.h"

#define PRIVATEBUFFER_INIT_CAPACITY \
			(2 * (PANDORA_REPARTITIONING_THRESHOLD / BLCKSZ))
#define MAX_SPREAD_FACTOR (32)
#define SIRO_TUPLE_LINEITEM_NUMS (3)

#define PandoraRecordsPerBlock(relation) \
			((relation)->records_per_block)

/*
 * We use this structure to store siro tuple's address and size. We access the
 * tuple using stored address and size.
 */
typedef struct SiroTupleData
{
	/* p-locator */
	uint32			plocatorLen;		/* length of *plocatorData */
	Item plocatorData;		/* tuple header and data */
	ItemPointer		plocatorSelf;

	/* Left version */
	uint32			leftVersionLen;		/* length of *leftVersionData */
	HeapTupleHeader leftVersionData;	/* tuple header and data */
	ItemPointer		leftVersionSelf;

	/* Right version */
	uint32			rightVersionLen;	/* length of *rightVersionData */
	HeapTupleHeader rightVersionData;	/* tuple header and data */
	ItemPointer		rightVersionSelf;

	/* Is p_pd_gen is same with pd_gen */
	bool			isUpdated;
} SiroTupleData;

typedef SiroTupleData *SiroTuple;

/*
 * When scanning partition files for repartitioning, repartitioning worker saves
 * scan status to this structure.
 */
typedef struct PandoraPartitionScanDesc 
{
	PandoraRelationMeta *meta;

	/* state set up at the beginning of repartitioning. */
	Relation			relation;
	PandoraPartLevel	partitionLevel;
	PandoraPartNumber	minPartitionNumber;
	PandoraPartNumber	maxPartitionNumber;

	/* state set up at the beginning of each partition scan. */
	PandoraPartNumber	currentPartitionNumber;
	PandoraPartGenNo	currentPartitionGenerationNumber;
	BlockNumber			nblocks;	/* total number of blocks in rel */
	BlockNumber			startBlock;	/* block # to start at */
	bool				firstScan;

	/* scan current state */
	BlockNumber		currentBlock;		/* current block # in scan, if any */
	Buffer			currentBuffer;		/* current buffer in scan, if any */
	SiroTuple		currentTuple;		/* current tuple in scan, if any */

	int				currentIndex;		/* current tuple's index in vistuples */
	int				ntuples;			/* number of visible tuples on page */
	SiroTupleData	siroTuples[MaxHeapTuplesPerPage];
	Buffer			tupleBuffers[MaxHeapTuplesPerPage * 2];
	int				pinOtherBufferNums;

	/*
	 * There are two types of objects that repartition: partitioning worker and
	 * checkpointer.
	 * 
	 * Normally, only the partitioning worker does the repartitioning, but when
	 * the PostgreSQL server shuts down, the checkpointer does the last
	 * repartitioning, and it only repartitions level 0, including the newest
	 * mempartition.
	 * 
	 * The type of object being repartitioned determines whether the newest
	 * mempartition is repartitioned or not, so a flag is needed to distinguish
	 * between the two.
	 */
	bool			isPartitioningWorker;

#ifdef PANDORA_DEBUG
	/* for debug */
	uint64				repartitionedNtuple_partition;
#endif /* PANDORA DEBUG */
} PandoraPartitionScanDesc;

/*
 * Private buffer is a local memory. Repartitioning worker appends tuples to
 * private buffer's 'pages' area. Also, the status of private buffer is stored 
 * here.
 */
typedef struct PandoraPartitionPrivateBuffer
{
	Page		*pages;		/* page array */
	int			npage;		/* current # of page */
	int			capacity;	/* page array capacity */

	/* private buffer current state */
	PandoraPartNumber	partitionNumber;			/* target partition number */
	PandoraPartGenNo	partitionGenerationNumber; /* target generation number */
	BlockNumber	nblocks;		/* total block number before partitioning. */
	int			firstRecCnt;	/* the # of tuple before partitioning. */
	int			currentRecCnt;	/* the # of tuple. */
	bool		partial;		/* does it have partial page? */

	/* only used in partial page */
	LocationIndex	prevLower;	/* previous lower bound */
	LocationIndex	prevUpper;	/* previous upper bound */
} PandoraPartitionPrivateBuffer;

/*
 * When moving upper partition's tuples to downside, repartitioning uses this 
 * insertion description.
 */
typedef struct PandoraPartitionInsertDesc
{
	/* insert current state */
	SiroTuple			insertTuple;		/* current tuple in scan, if any */
	PandoraPartNumber	targetPartitionNumber; /* partition number for insertion */
	PandoraPartLevel	upperPartitionLevel; /* source partition level */
	PandoraPartLevel	targetPartitionLevel; /* destination partition level */

	/* private memory space */
	PandoraPartitionPrivateBuffer **privateBuffers;
	int 	nprivatebuffer;		/* the # of private buffer space */
	int		startTargetPartitionNumber;

	/* the # of partition which one target partition covers.*/
	int 	targetPartitionCoverage;	
} PandoraPartitionInsertDesc;

/*
 * Pandora's repartitioning description
 */
typedef struct PandoraRepartitioningDesc 
{
	/* state set up at the beginning of repartitioning. */
	PandoraRelationMeta *meta;
	Relation			relation;

	/* upper partition info */
	PandoraPartitionScanDesc upperScan;

	/* insert info */
	PandoraPartitionInsertDesc lowerInsert;

	/* for debugging. */
	uint64 deltaNums;

	uint64	parentBlocks;
	uint64	childBlocks;

#ifdef PANDORA_MEASURE
	struct timespec startTime;	
	struct timespec endTime;
	struct timespec copyStartTime;
	struct timespec copyEndTime;
	struct timespec blockingStartTime;
#endif
} PandoraRepartitioningDesc;

#define PANDORA_DELTA_PAGE_SIZE (2 * 1024 * 1024) /* 2 MiB */

/*
 * PandoraDelta
 *
 * TODO: comment
 */
typedef struct PandoraDelta
{
	/* member variables to search tuple from upper partition. */
	int upperTuplePosition; /* used for finding offset in partition file. */
	PandoraPartNumber partitionNumber; 

	/* member variables to copy tuple to lower target partition. */
	int partitionId;		/* used for finding target partition. */
	int64_t lowerTuplePosition; /* used for finding offset in partition file. */
} PandoraDelta;

/*
 * PandoraGarbageReservedDeltaSpace
 */
typedef struct PandoraGarbageReservedDeltaSpace
{
	dsa_pointer currentItem; /* delta space to do garbage collection. */
	dsa_pointer next; 		 /* next 'PandoraGarbageReservedDeltaSpace'. */
} PandoraGarbageReservedDeltaSpace;

/*
 * PandoraDeltaSpace
 */
typedef struct PandoraDeltaSpace
{
	int nhugepage;

	/* Page size is the same 2 MiB. */
	dsa_pointer hugepages[FLEXIBLE_ARRAY_MEMBER]; /* should be last member. */
} PandoraDeltaSpace;

/*
 * PandoraDeltaSpaceEntry
 * 
 * TODO: comment
 */
typedef struct PandoraDeltaSpaceEntry
{
	int SizePerHugepage;			/* bytes unit */
	int deltaNumsPerHugepage;

	pg_atomic_uint64 deltaNums;		/* delta nums */
	pg_atomic_uint64 deltaCapacity;	/* delta space's capacity */

	/* PandoraDeltaSpace */
	dsa_pointer 	 deltaSpace;

	/* PandoraGarbageReservedDeltaSpace */
	dsa_pointer		 reservedForGarbageCollection;
} PandoraDeltaSpaceEntry;

/* Public functions. */

/* backend/pandora/partitioning/pandora_partitioning.c */
extern dsa_pointer PandoraPartitioningInit(dsa_area* area); 
extern SiroTuple PandoraPartitionGetTuple(PandoraPartitionScanDesc *pscan);
extern void PandoraPartitionGetPage(PandoraPartitionScanDesc *pscan, 
														BlockNumber page);
extern void PandoraRepartitioning(PandoraRepartitioningDesc *desc,
								  Oid relationOid,
								  uint64 recordCount,
								  PandoraPartLevel partitionLevel,
								  PandoraPartNumber minPartitionNumber,
								  PandoraPartNumber maxPartitionNumber,
								  PandoraPartLevel targetPartitionLevel,
								  uint32 *reorganizedRecCnt,
								  PandoraSeqNum *firstSeqNums,
								  bool isPartitioningWorker);
extern uint32 PandoraPartitionAppend(PandoraRepartitioningDesc *desc, 
									 SiroTuple tuple,
					   				 PandoraSeqNum *firstSeqNums);
extern PandoraPartNumber GetPartitionNumberFromPartitionId(int spreadFactor, 
													PandoraPartLevel destLevel, 
													PandoraPartLevel lastLevel, 
													int partitionId);

/* backend/pandora/partitioning/pandora_partitioning_delta.c */
extern dsa_pointer CreatePandoraDeltaSpaceEntry(dsa_area *area);
extern void DeletePandoraDeltaSpaceEntry(dsa_area *area, 
										dsa_pointer dsaDeltaSpaceEntry);
extern void AppendPandoraDelta(dsa_area *area, PandoraRelationMeta *meta, 
											PandoraTuplePosition tuplePosition, 
											bool isLevelZeroOne);
extern void ApplyPandoraDeltas(dsa_area *area, PandoraRepartitioningDesc *desc);
extern void RepartitionNewestMempartition(int flags);

#endif /* PANDORA_PARTITIONING_H */
