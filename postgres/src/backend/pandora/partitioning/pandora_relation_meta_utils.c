/*-------------------------------------------------------------------------
 *
 * pandora_relation_meta_utils.c
 *
 * Pandora Relation Meta Util Implementation
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

#ifdef PANDORA
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
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dynahash.h"
#include "utils/resowner_private.h"

#include "pandora/pandora.h"
#include "pandora/pandora_relation_meta.h"

#define MAX_RELATION_META (32)

/*
 * PandoraGetTotalPartitionNums
 */
int 
PandoraGetTotalPartitionNums(PandoraRelationMeta *meta, bool includeLastLevel)
{
	int totalPartitionNums = 0;

	for (int level = 1; level <= meta->lastPartitionLevel; level++)
	{
		if (level == meta->lastPartitionLevel && !includeLastLevel)
			break;

		totalPartitionNums += (int) pow(meta->spreadFactor, level);
	}

	return totalPartitionNums;
}

/*
 * PandoraGetPartGenNo
 */
PandoraPartGenNo
PandoraGetPartGenerationNumber(PandoraRelationMeta *meta, 
								PandoraPartLevel partitionLevel, 
								PandoraPartNumber partitionNumber)
{
	PandoraPartGenNo generationNumber;

	if (partitionLevel == 0 || partitionLevel == meta->lastPartitionLevel)
		generationNumber = 0;
	else
		generationNumber = meta->generationNums[partitionLevel][partitionNumber];

	return generationNumber;
}


/*
 * IsPartitionOldFile
 *
 * Is old partition file?
 */
bool 
IsPartitionOldFile(Oid relationOid, PandoraPartLevel partitionLevel, 
					PandoraPartNumber partitionNumber, 
					PandoraPartGenNo generationNumber)
{
	PandoraRelationMeta *meta = PandoraGetRelationMeta(relationOid);
	PandoraPartGenNo currentGenerationNumber;

	Assert(meta->inited);
	Assert(partitionLevel > 0 && partitionLevel <= meta->lastPartitionLevel);

	currentGenerationNumber = 
		PandoraGetPartGenerationNumber(meta, 
											partitionLevel, partitionNumber);

	//TODO: Add logic to check whether the target is currently undergoing partitioning or not.

	return currentGenerationNumber == generationNumber ? false : true;
}

/*
 * IncrementPandoraSequenceNumber 
 *
 * Return value is old one.
 */
uint64
IncrementSequenceNumber(PandoraRelationMeta *meta)
{
	Assert(meta->inited == true);

	return pg_atomic_fetch_add_u64(meta->sequenceNumberCounter, 1);
}
#endif /* PANDORA */
