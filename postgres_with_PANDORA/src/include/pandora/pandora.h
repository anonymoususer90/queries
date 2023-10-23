/*-------------------------------------------------------------------------
 *
 * pandora.h
 *	  Fundamental definitions for PANDORA.
 *
 *
 * src/include/pandora/pandora.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PANDORA_H
#define PANDORA_H

#include "c.h"

typedef uint32 PandoraPartLevel;
typedef uint32 PandoraPartNumber;
typedef uint32 PandoraPartGenNo;
typedef uint64 PandoraSeqNum;

#define InvalidPandoraPartLevel		((PandoraPartLevel)		0xFFFFFFFF)
#define InvalidPandoraPartNumber	((PandoraPartNumber)	0xFFFFFFFF)
#define InvalidPandoraPartGenNo		((PandoraPartGenNo)		0xFFFF)
#define InvalidPandoraSeqNum		((PandoraSeqNum)		0xFFFFFFFFFFFFFFFF)

#define CUMULATIVECOUNT_MASK		(0x0000000000FFFFFF) /* 3-bytes */

typedef struct PandoraRecordKey
{
	uint64 partid: 24,
		   seqnum: 40;
} PandoraRecordKey;

typedef struct PandoraInPartCnt
{
	uint32 cumulative_count:24,
		   padding: 8; /* */
} PandoraInPartCnt;

typedef struct PandoraRecordPositions
{
	PandoraPartNumber first_lvl_partnum;
	uint32 first_lvl_pos;
	uint64 last_lvl_pos;
	PandoraInPartCnt in_partition_counts[FLEXIBLE_ARRAY_MEMBER];
} PandoraRecordPositions;

typedef struct PandoraFingerprintData
{
	PandoraRecordKey		rec_key;
	PandoraRecordPositions	rec_pos;
} PandoraFingerprintData;

typedef struct PandoraFingerprintData *PandoraFingerprint;


/* modification method 
 *
 * ====================== Repartitioning Worker Flow =========================
 *
 * (1) repartitioning                   (2)  set            (3) repartitioning
 *         start                           blocking                  end
 *           ↓                                ↓                       ↓
 * Time -------------------------------------------------------------------->
 *               ↑           ↑           ↑             ↑            ↑
 *                MODIFICATION_WITH_DELTA           MODIFICATION_BLOCKING 
 *                           
 * ====================== Transactions for Modification ======================
 */
typedef enum {
	NORMAL_MODIFICATION = 1, 	/* repartitioning is not inprogress. */
	MODIFICATION_WITH_DELTA,
	MODIFICATION_BLOCKING
} PandoraModificationMethod;

/*
 * PandoraTuplePosition
 * 		We save the position of tuple within this struct. Partition level, 
 *		partition number, partition generation number, and real tuple position
 *		are saved. In this context, partitionTuplePosition represents a tuple 
 *		count, not a byte unit.
 */
typedef struct PandoraTuplePositionData
{
	PandoraPartLevel partitionLevel;	/* partition level to access */
	PandoraPartNumber partitionNumber;	/* partition number to access */
	PandoraPartGenNo partitionGenerationNumber; /* partition generation number */

	/* the selected position in the target partition */
	PandoraSeqNum partitionTuplePosition;

	PandoraRecordKey rec_key; /* For assertion */

	/* status for modification delta */
	bool needDecrRefCount;
	PandoraModificationMethod modificationMethod;
	int partitionId;
	PandoraSeqNum lowerPartitionTuplePosition;
} PandoraTuplePositionData;

typedef struct PandoraTuplePositionData *PandoraTuplePosition;

#define SizeOfPandoraFingerprintData(lvl_cnt) \
	(sizeof(PandoraRecordKey) + \
	 offsetof(PandoraRecordPositions, in_partition_counts) + \
	 ((lvl_cnt - 2) * sizeof(PandoraInPartCnt)))

#define NBlocksUsingRecordCount(rec_cnt, rec_per_block) \
	(((rec_cnt - 1) / rec_per_block) + 1)
#define LastRecordCountUsingRecordCount(rec_cnt, rec_per_block) \
	(((rec_cnt - 1) % rec_per_block) + 1)
#define GetPandoraFingerprint(itup) \
	((PandoraFingerprint)(((char*)itup)+(itup->t_fingerprint_offset)))

#endif /* PANDORA_H */
