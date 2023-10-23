/*
 * pandora_partitioning_delta.c
 *
 * Pandora Paritioning Delta Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/pandora/partitioning/pandora_partitioning_delta.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef PANDORA

#include "postgres.h"
#include "utils/dsa.h"
#include "storage/bufmgr.h"
#include "pandora/pandora_partitioning.h"
#include "pandora/pandora_relation_meta.h"
#include "postmaster/pandora_partition_mgr.h"

static PandoraGarbageReservedDeltaSpace *
PandoraConvertToGarbageReservedDeltaSpace(dsa_area* area, dsa_pointer ptr)
{
	return (PandoraGarbageReservedDeltaSpace *) dsa_get_address(area, ptr);
}
 
static PandoraDeltaSpace *
PandoraConvertToDeltaSpace(dsa_area* area, dsa_pointer ptr)
{
	return (PandoraDeltaSpace *) dsa_get_address(area, ptr);
}

static PandoraDeltaSpaceEntry *
PandoraConvertToDeltaSpaceEntry(dsa_area* area, dsa_pointer ptr)
{
	return (PandoraDeltaSpaceEntry *) dsa_get_address(area, ptr);
}

static Page
PandoraConvertToHugepage(dsa_area *area, dsa_pointer ptr)
{
	return (Page) dsa_get_address(area, ptr);
}

/*
 * CreatePandoraDeltaSpace
 */
static dsa_pointer
CreatePandoraDeltaSpace(dsa_area *area, int nhugepage)
{
	dsa_pointer dsaDeltaSpace;
	PandoraDeltaSpace *deltaSpace;
 
	dsaDeltaSpace = 
		dsa_allocate_extended(area, 
			sizeof(PandoraDeltaSpace) + sizeof(dsa_pointer) * nhugepage, 
														DSA_ALLOC_ZERO);

	deltaSpace = PandoraConvertToDeltaSpace(area, dsaDeltaSpace);
	deltaSpace->nhugepage = nhugepage;

	return dsaDeltaSpace;
}

/*
 * CreatePandoraDeltaSpaceEntry
 */
dsa_pointer
CreatePandoraDeltaSpaceEntry(dsa_area *area)
{
	int nhugepage = 1; /* init value */
	dsa_pointer dsaDeltaSpaceEntry;
	PandoraDeltaSpaceEntry *deltaSpaceEntry;
	PandoraDeltaSpace *deltaSpace;

	/* dsa allocation. */
	dsaDeltaSpaceEntry =
		dsa_allocate_extended(area, sizeof(PandoraDeltaSpaceEntry), DSA_ALLOC_ZERO);

	/* Change dsa address. */
	deltaSpaceEntry = PandoraConvertToDeltaSpaceEntry(area, dsaDeltaSpaceEntry);

	/* Init values */
	deltaSpaceEntry->SizePerHugepage = PANDORA_DELTA_PAGE_SIZE;
	deltaSpaceEntry->deltaNumsPerHugepage = 
		(deltaSpaceEntry->SizePerHugepage / sizeof(PandoraDelta));

	pg_atomic_init_u64(&deltaSpaceEntry->deltaNums, 0);
	pg_atomic_init_u64(&deltaSpaceEntry->deltaCapacity, 
									deltaSpaceEntry->deltaNumsPerHugepage);
	
	deltaSpaceEntry->deltaSpace = CreatePandoraDeltaSpace(area, nhugepage); 
	deltaSpaceEntry->reservedForGarbageCollection = InvalidDsaPointer;

	/* Add one hugepage to delta space. */
	deltaSpace = PandoraConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);
	deltaSpace->hugepages[0] = dsa_allocate(area, PANDORA_DELTA_PAGE_SIZE);

	return dsaDeltaSpaceEntry;
}

/*
 * DeletePandoraDeltaSpaceEntry
 */
void
DeletePandoraDeltaSpaceEntry(dsa_area *area, dsa_pointer dsaDeltaSpaceEntry)
{
	dsa_pointer garbageTarget;
	PandoraDeltaSpace *deltaSpace;
	PandoraDeltaSpaceEntry *deltaSpaceEntry;

	if (!DsaPointerIsValid(dsaDeltaSpaceEntry))
	{
		elog(WARNING, "try to free dangling dsa pointer.");
		return;
	}

	deltaSpaceEntry = PandoraConvertToDeltaSpaceEntry(area, dsaDeltaSpaceEntry);
	deltaSpace = PandoraConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Delete delta space's hugepages. */
	for (int i = 0; i < deltaSpace->nhugepage; ++i)
		dsa_free(area, deltaSpace->hugepages[i]);

	dsa_free(area, deltaSpaceEntry->deltaSpace);

	/* Do garbage collection */
	garbageTarget = deltaSpaceEntry->reservedForGarbageCollection;

	while (DsaPointerIsValid(garbageTarget))
	{
		PandoraGarbageReservedDeltaSpace *garbageReservedDeltaSpace;
		dsa_pointer nextGarbageTarget;

		/* Get structure. */
		garbageReservedDeltaSpace = 
			PandoraConvertToGarbageReservedDeltaSpace(area, garbageTarget);

		/* Get next garbage target's dsa pointer. */
		nextGarbageTarget = garbageReservedDeltaSpace->next;

		/* Resource cleanup. */
		dsa_free(area, garbageReservedDeltaSpace->currentItem);
		dsa_free(area, garbageTarget);

		/* Move to next item for garbage collection. */
		garbageTarget = nextGarbageTarget;
	}

	/* Resource cleanup. */
	dsa_free(area, dsaDeltaSpaceEntry);
}

/*
 * GetPandoraDelta
 */
static PandoraDelta *
GetPandoraDelta(dsa_area *area, PandoraDeltaSpace *deltaSpace, 
						Index deltaIdx, int deltaNumsPerHugepage)
{
	Page hugepage;
	Index hugepageIdx;
	off_t offset;
	PandoraDelta *delta;

	/* Calculate target hugepage and index. */
	hugepageIdx = deltaIdx / deltaNumsPerHugepage;
	offset = deltaIdx % deltaNumsPerHugepage;
	Assert(hugepageIdx < deltaSpace->nhugepage);

	/* Get hugepage. */
	hugepage = PandoraConvertToHugepage(area, deltaSpace->hugepages[hugepageIdx]);

	/* Get delta from hugepage. */
	delta = (PandoraDelta *) ((char *) hugepage + sizeof(PandoraDelta) * offset); 

	return delta;
}

/*
 * PandoraDeltaGetTuple
 */
static void
PandoraDeltaGetTuple(PandoraRepartitioningDesc *desc,
					PandoraPartLevel partitionLevel, 
					PandoraPartNumber partitionNumber,
					PandoraPartGenNo partitionGenerationNumber,
					Buffer *buffer, int upperTuplePosition, SiroTuple siroTuple)
{
	PandoraRelationMeta *meta = desc->meta;
	Relation 	relation = desc->relation;
	BlockNumber blockNumber;
	OffsetNumber offsetNumber;
	ItemId plocatorLpp, leftLpp, rightLpp;
	Page page;

	Assert(*buffer == InvalidBuffer);

	/* Calculate target block number. */
	blockNumber = upperTuplePosition / meta->recordNumsPerBlock;
	offsetNumber = (upperTuplePosition % meta->recordNumsPerBlock) * 3 + 1;

	/* Read target page. */
	*buffer = ReadPartitionBufferExtended(relation, 
										partitionLevel, 
										partitionNumber, 
										partitionGenerationNumber,
										blockNumber, RBM_NORMAL, NULL);
	page = BufferGetPage(*buffer);

	/* 
	 * XXX: In the blocking phase, there are no updates to this page. However, 
	 * we acquire a lock to prevent modifications, such as those performed by 
	 * vacuum processes.. 
 	 */
	LockBuffer(*buffer, BUFFER_LOCK_SHARE);

	/* Set plocator info. */
	plocatorLpp = PageGetItemId(page, offsetNumber);
	siroTuple->plocatorData = PageGetItem(page, plocatorLpp);
	siroTuple->plocatorLen = ItemIdGetLength(plocatorLpp);

	/* Set left version info. */
	leftLpp = PageGetItemId(page, offsetNumber + 1);
	siroTuple->leftVersionData = (HeapTupleHeader) PageGetItem(page, leftLpp);
	siroTuple->leftVersionLen = ItemIdGetLength(leftLpp);

	/* Set right version info. */
	rightLpp = PageGetItemId(page, offsetNumber + 2);
	siroTuple->rightVersionData = (HeapTupleHeader) PageGetItem(page, rightLpp);
	siroTuple->rightVersionLen = ItemIdGetLength(rightLpp);

	LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
}

/*
 * PandoraWriteDeltaToPrivateBuffer
 */
static void
PandoraWriteDeltaToPrivateBuffer(PandoraRepartitioningDesc *desc, 
						PandoraPartLevel targetPartitionLevel, int partitionId, 
						int64_t lowerTuplePosition, SiroTuple siroTuple)
{
	PandoraPartitionInsertDesc *insertDesc = &(desc->lowerInsert);
	PandoraRelationMeta	*meta = desc->meta;
	ItemPointer		l_pointer = NULL;
	ItemPointer		r_pointer = NULL;
	PandoraPartNumber 	lowerPartitionNumber; 
	PandoraPartitionPrivateBuffer *privateBuffer;
	Index 			privateBufferIdx;
	BlockNumber 	targetBlockNumber;
	Index 			pageIdx;
	OffsetNumber 	targetOffsetNumber;
	ItemId 			plocatorLpp, leftLpp, rightLpp;
	Page			page;
	PageHeader		phdr;
	uint32		   *p_pd_gen;
	Item			plocator;

	/* Get target partition number. */
	lowerPartitionNumber =
		GetPartitionNumberFromPartitionId(meta->spreadFactor, 
				targetPartitionLevel, meta->lastPartitionLevel, partitionId);

	/* Get index on private buffers to modify the tuple. */
	privateBufferIdx = lowerPartitionNumber % insertDesc->nprivatebuffer;
	privateBuffer = insertDesc->privateBuffers[privateBufferIdx];

	/* Calculate block number and offset that the tuple should be modified. */
	targetBlockNumber = lowerTuplePosition / meta->recordNumsPerBlock;
	targetOffsetNumber = (lowerTuplePosition % meta->recordNumsPerBlock) * 3 + 1;

	/* Calculate page index in private buffer's pages. */
	if (privateBuffer->firstRecCnt == 0)
	{
		pageIdx = targetBlockNumber;
	}
	else
	{
		BlockNumber lastBlockNumberBeforePartitioning =
				((privateBuffer->firstRecCnt - 1) / meta->recordNumsPerBlock);

		Assert(lastBlockNumberBeforePartitioning <= targetBlockNumber);

		/* Calculate page index. */
		pageIdx = targetBlockNumber - lastBlockNumberBeforePartitioning;
		if (privateBuffer->partial == false)
			pageIdx -= 1;
	}
	
	/* 
	 * Get page in the private buffer. The repartitioning worker needs to apply 
	 * delta to this page.
	 */
	Assert(pageIdx < privateBuffer->npage);
	page = privateBuffer->pages[pageIdx];
	phdr = (PageHeader) page;

	/* Get lineitems. */
	plocatorLpp = PageGetItemId(page, targetOffsetNumber);
	leftLpp = PageGetItemId(page, targetOffsetNumber + 1);
	rightLpp = PageGetItemId(page, targetOffsetNumber + 2);
	
	/* Fix lineitems (= slot array). */
	plocatorLpp->lp_len = siroTuple->plocatorLen;
	leftLpp->lp_len = siroTuple->leftVersionLen;
	rightLpp->lp_len = siroTuple->rightVersionLen;

	/* Get p-locator item. */
	plocator = PageGetItem(page, plocatorLpp);
	Assert(PGetPandoraRecordKey(plocator)->partid == partitionId);
	Assert(PGetPandoraRecordKey(plocator)->seqnum == 
				PGetPandoraRecordKey(siroTuple->plocatorData)->seqnum);
	
	/* Fix tuple. */
	memcpy(plocator, siroTuple->plocatorData, siroTuple->plocatorLen);
	memcpy(PageGetItem(page, leftLpp), siroTuple->leftVersionData, 
											siroTuple->leftVersionLen);
	memcpy(PageGetItem(page, rightLpp), siroTuple->rightVersionData, 
											siroTuple->rightVersionLen);

	pg_memory_barrier();

	/* We need to change block number for new partition. */
	l_pointer = (ItemPointer) (plocator + PITEM_PTR_OFF);
	r_pointer = (ItemPointer) (plocator + PITEM_PTR_OFF + sizeof(ItemPointerData));
	ItemPointerSet(l_pointer, targetBlockNumber, targetOffsetNumber + 1);
	ItemPointerSet(r_pointer, targetBlockNumber, targetOffsetNumber + 2);

	/* We need to change pd gen for new partition. */
	p_pd_gen = (uint32 *) (plocator + PITEM_GEN_OFF);
	*p_pd_gen = phdr->pd_gen;
}

/*
 * ApplyPandoraDeltas
 * 
 * TODO: comment
 */
void
ApplyPandoraDeltas(dsa_area *area, PandoraRepartitioningDesc *desc)
{
	dsa_pointer dsaPandoraPartitionDeltaSpaceEntry;
	PandoraPartitionScanDesc *upperScan = &(desc->upperScan);
	PandoraPartitionInsertDesc *insertDesc = &(desc->lowerInsert);
	PandoraRelationMeta *meta = desc->meta;
	PandoraPartLevel upperPartitionLevel, lowerPartitionLevel;
	PandoraPartGenNo upperPartitionGenerationNumber;
	PandoraDeltaSpaceEntry *deltaSpaceEntry;
	PandoraDeltaSpace *deltaSpace;
	uint64 deltaNums;
	uint64 deltaIdx;

	/* Get upper partition level and lower patition level. */
	upperPartitionLevel = upperScan->partitionLevel;
	lowerPartitionLevel = insertDesc->targetPartitionLevel;

	/* Get correct delta space entry. */
	if (upperPartitionLevel <= 1)
	{
		dsaPandoraPartitionDeltaSpaceEntry = 
					meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne;
	}
	else
	{
		dsaPandoraPartitionDeltaSpaceEntry = 
					meta->dsaPartitionDeltaSpaceEntryForOthers;
	}

	/* Get delta space structure. */
	deltaSpaceEntry =
		PandoraConvertToDeltaSpaceEntry(area, dsaPandoraPartitionDeltaSpaceEntry);
	deltaSpace =
		PandoraConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Get the total number of delta. */
	deltaNums = pg_atomic_read_u64(&deltaSpaceEntry->deltaNums);
	if (deltaNums == 0)
	{
		/* If there are no deltas that need to be applied, simply return. */
		return;
	}

	/* for debugging. */
	desc->deltaNums = deltaNums;

	/* Iterate all deltas. */
	for (deltaIdx = 0; deltaIdx < deltaNums; ++deltaIdx)
	{
		Buffer buffer = InvalidBuffer;
		PandoraDelta *delta;
		SiroTupleData siroTupleData;

		/* Get a delta using delta idx. */
		delta = GetPandoraDelta(area, deltaSpace, deltaIdx, 
								deltaSpaceEntry->deltaNumsPerHugepage);

		/* 
		 * Get upper partition's generation number. Caller should increment 
		 * metadata bit before it.
		 */
		upperPartitionGenerationNumber = 
			PandoraGetPartGenerationNumber(meta, upperPartitionLevel, 
												delta->partitionNumber);

		/* Get a modified siro tuple from shared buffer. */
		PandoraDeltaGetTuple(desc, upperPartitionLevel,
							delta->partitionNumber,
							upperPartitionGenerationNumber,
							&buffer, delta->upperTuplePosition, &siroTupleData);
		Assert(PGetPandoraRecordKey(siroTupleData.plocatorData)->partid == 
															delta->partitionId);

		/* Write modified siro tuple to private buffer. */
		PandoraWriteDeltaToPrivateBuffer(desc, lowerPartitionLevel,
											delta->partitionId, 
											delta->lowerTuplePosition, 
											&siroTupleData);

		Assert(BufferIsValid(buffer));
		ReleaseBuffer(buffer);
	}
}


/*
 * CreatePandoraGarbageReservedDeltaSpace
 */
static dsa_pointer
CreatePandoraGarbageReservedDeltaSpace(dsa_area *area, 
									dsa_pointer garbagedeltaSpace)
{
	dsa_pointer dsaGarbageReservedDeltaSpace;
	PandoraGarbageReservedDeltaSpace *garbageReservedDeltaSpace;

	/* dsa allocation. */
	dsaGarbageReservedDeltaSpace =
		dsa_allocate(area, sizeof(PandoraGarbageReservedDeltaSpace));

	/* Change dsa address. */
	garbageReservedDeltaSpace = 
		PandoraConvertToGarbageReservedDeltaSpace(area, 
												dsaGarbageReservedDeltaSpace);

	/* Init values. */
	garbageReservedDeltaSpace->currentItem = garbagedeltaSpace;
	garbageReservedDeltaSpace->next = InvalidDsaPointer;

	return dsaGarbageReservedDeltaSpace;
}

/*
 * CopyToDeltaSpace
 */
static void
CopyToDeltaSpace(dsa_area *area, dsa_pointer dsaFromDeltaSpace, 
									dsa_pointer dsaToDeltaSpace)
{
	PandoraDeltaSpace *fromDeltaSpace =
		PandoraConvertToDeltaSpace(area, dsaFromDeltaSpace);
	PandoraDeltaSpace *toDeltaSpace =
		PandoraConvertToDeltaSpace(area, dsaToDeltaSpace);

	/* We assume that copying will be performed into a larger array. */
	Assert(fromDeltaSpace->nhugepage < toDeltaSpace->nhugepage);
	
	memcpy(&toDeltaSpace->hugepages, &fromDeltaSpace->hugepages, 
					sizeof(dsa_pointer) * fromDeltaSpace->nhugepage);
}

/*
 * ExpandPandoraDeltaSpace
 */
static void
ExpandPandoraDeltaSpace(dsa_area *area, PandoraDeltaSpaceEntry *deltaSpaceEntry)
{
	uint64 curDeltaCapacity = pg_atomic_read_u64(&deltaSpaceEntry->deltaCapacity);
	dsa_pointer dsaGarbageReservedDeltaSpace;
	dsa_pointer dsaNewDeltaSpace;
	PandoraDeltaSpace *newDeltaSpace;
	PandoraGarbageReservedDeltaSpace *garbageReservedDeltaSpace;
	int curHugepageNums, nextHugepageNums;

	/* Create wrapper for garbage collection later. */
 	dsaGarbageReservedDeltaSpace =
		CreatePandoraGarbageReservedDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Get structure. */
	garbageReservedDeltaSpace = 
		PandoraConvertToGarbageReservedDeltaSpace(area, 
							dsaGarbageReservedDeltaSpace);

	/* Set next garbage wrapper. */
	garbageReservedDeltaSpace->next = 
		deltaSpaceEntry->reservedForGarbageCollection;

	/*
	 * Only one worker is responsible for expanding the delta space, and we 
	 * ensure that there is no need for concurrency control in this process.
	 */
	deltaSpaceEntry->reservedForGarbageCollection = dsaGarbageReservedDeltaSpace;

	/* Get current hugepage number. */
	curHugepageNums = curDeltaCapacity / deltaSpaceEntry->deltaNumsPerHugepage;

	/* Get next hugepage number. */
	nextHugepageNums = curHugepageNums + 1;
	Assert((curDeltaCapacity % deltaSpaceEntry->deltaNumsPerHugepage) == 0);

	/* Create expanded delta space. */
	dsaNewDeltaSpace = CreatePandoraDeltaSpace(area, nextHugepageNums);

	/* Copy existing hugepage's dsa pointers to new one. */
	CopyToDeltaSpace(area, deltaSpaceEntry->deltaSpace, dsaNewDeltaSpace);

	/* Add one hugepage to delta space. */
	newDeltaSpace = PandoraConvertToDeltaSpace(area, dsaNewDeltaSpace);
	Assert(newDeltaSpace->hugepages[nextHugepageNums - 1] == 0);
	newDeltaSpace->hugepages[nextHugepageNums - 1] = 
			dsa_allocate(area, PANDORA_DELTA_PAGE_SIZE);

	/* Change current delta space to new one. */
	deltaSpaceEntry->deltaSpace = dsaNewDeltaSpace;

	pg_memory_barrier();
	pg_atomic_fetch_add_u64(&deltaSpaceEntry->deltaCapacity, 
								deltaSpaceEntry->deltaNumsPerHugepage);
}

/*
 * AppendPandoraDeltaInternal
 */
static void
AppendPandoraDeltaInternal(dsa_area *area, 
						PandoraDeltaSpaceEntry *deltaSpaceEntry, 
						uint64_t deltaPosition, PandoraDelta *delta) 
{
	PandoraDeltaSpace *deltaSpace;
	Index hugepageIdx;
	off_t offset;
	Page hugepage;

	/* Get delta space structure.  */
	deltaSpace = PandoraConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Calculate delta position to write. */
	hugepageIdx = deltaPosition / deltaSpaceEntry->deltaNumsPerHugepage;
	offset = deltaPosition % deltaSpaceEntry->deltaNumsPerHugepage;
	Assert(hugepageIdx < deltaSpace->nhugepage);

	/* Get hugepage pointer. */
	hugepage = 
		PandoraConvertToHugepage(area, deltaSpace->hugepages[hugepageIdx]);

	/* Write delta. */
	memcpy((char *) hugepage + sizeof(PandoraDelta) * offset, delta, 
												sizeof(PandoraDelta));
}

/*
 * AppendPandoraDelta
 *
 * TODO: comment
 */
void
AppendPandoraDelta(dsa_area *area, PandoraRelationMeta *meta, 
			PandoraTuplePosition tuplePosition, bool isLevelZeroOne)
{
	dsa_pointer dsaDeltaSpaceEntry;
	PandoraDeltaSpaceEntry *deltaSpaceEntry;
	PandoraDelta modificationDelta;
	uint64_t deltaPosition;

	/* Get correct delta space entry. */
	if (isLevelZeroOne)
 		dsaDeltaSpaceEntry = meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne;
	else
		dsaDeltaSpaceEntry = meta->dsaPartitionDeltaSpaceEntryForOthers;
		
	/* Get delta space entry from the relation meta. */
	deltaSpaceEntry = PandoraConvertToDeltaSpaceEntry(area, dsaDeltaSpaceEntry);

	/* Increment and get delta position atomically. */
	deltaPosition = pg_atomic_fetch_add_u64(&deltaSpaceEntry->deltaNums, 1);

	/*
	 * If the delta position exceeds the capacity, we must wait for 
	 * someone to expand the delta space.
	 */
	for (;;)
	{
		uint64_t deltaCapacity = 
			pg_atomic_read_u64(&deltaSpaceEntry->deltaCapacity);

		if (deltaCapacity < deltaPosition)
		{
			if (deltaCapacity + 1 == deltaPosition)
			{
				/* The worker has responsibility to expand delta space. */
				ExpandPandoraDeltaSpace(area, deltaSpaceEntry);
				break;
			}

			/* Wait for expanding. */
			SPIN_DELAY();
		}
		else
		{
			/* We can store delta safely. */
			break;
		}
	}

	pg_memory_barrier();

	/* 
	 * Append delta directly. In here, we can store delta to delta space 
	 * safely. 
	 */

	/* Init values. */
	modificationDelta.upperTuplePosition = 
		tuplePosition->partitionTuplePosition;
	modificationDelta.partitionNumber = tuplePosition->partitionNumber;
	modificationDelta.partitionId = tuplePosition->partitionId;
	modificationDelta.lowerTuplePosition = 
		tuplePosition->lowerPartitionTuplePosition;

	/* Write delta to delta space. */
	AppendPandoraDeltaInternal(area, deltaSpaceEntry, 
								deltaPosition, &modificationDelta);
}

#endif /* PANDORA */
