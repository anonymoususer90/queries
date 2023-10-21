/*-------------------------------------------------------------------------
 *
 * pandora_relation_meta_hash.c
 *
 * Hash table implementation for mapping relation structure.
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
 *    src/backend/pandora/partitioning/pandora_relation_hash.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef PANDORA
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dynahash.h"
#include "utils/hsearch.h"
#include "utils/snapmgr.h"

#include "pandora/pandora_relation_meta.h"

static HTAB *PandoraSharedRelationMetaHash;

/*
 * PandoraRelationMetaHashShmemSize
 *
 * compute the size of shared memory for pandora relation meta hash
 */
Size
PandoraRelationMetaHashShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(PandoraRelationMetaLookupEntry));
}

/*
 * PandoraRelationMetaHashInit
 *
 * Initialize pandora's relation meta hash in shared memory
 */
void
PandoraRelationMetaHashInit(int size)
{
	HASHCTL info;
	long num_partitions;

	/* See next_pow2_long(long num) in dynahash.c */
	num_partitions = 1L << my_log2(NUM_PANDORA_RELATION_META_PARTITIONS);

	/* PandoraRelationTag maps to PandoraSharedRelationHash */
	info.keysize = sizeof(PandoraRelationMetaTag);
	info.entrysize = sizeof(PandoraRelationMetaLookupEntry);
	info.num_partitions = num_partitions;

	PandoraSharedRelationMetaHash = ShmemInitHash(
									  "Shared Pandora Relation Meta Lookup Table",
									  size,
									  size,
									  &info,
									  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 * PandoraRelationMetaHashCode
 *
 * Compute the hash code associated with a PandoraRelationMetaTag
 * This must be passed to the lookup/insert/delete routines along with the
 * tag. We do this way because the callers need to know the hash code in
 * order to determine which buffer partition to lock, and we don't want to
 * do the hash computation twice (hash_any is a bit slow).
 */
uint32
PandoraRelationMetaHashCode(const PandoraRelationMetaTag *tagPtr)
{
	return get_hash_value(PandoraSharedRelationMetaHash, (void *) tagPtr);
}

/*
 * PandoraRelationMetaHashLookup
 * 
 * Lookup the given PandoraRelationMetaTag; return pandora relation meta index, 
 * or -1 if not found. Caller must hold at least shared lock on partition lock 
 * for tag's partition.
 */
int
PandoraRelationMetaHashLookup(const PandoraRelationMetaTag *tagPtr, 
															uint32 hashcode)
{
	PandoraRelationMetaLookupEntry *result;

	result = (PandoraRelationMetaLookupEntry *) hash_search_with_hash_value(
		PandoraSharedRelationMetaHash, 
		(void *) tagPtr, hashcode, HASH_FIND, NULL);

	if (!result) 
		return -1;

	return result->id;
}

/*
 * PandoraRelationMetaHashInsert
 *
 * Insert a hashtable entry for given tag and index,
 * unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion. If a conflicting entry already exists,
 * returns its buffer ID.
 *
 * Caller must hold exclusive lock on tag's partition
 */
int
PandoraRelationMetaHashInsert(const PandoraRelationMetaTag *tagPtr, 
													uint32 hashcode, int index)
{
	PandoraRelationMetaLookupEntry *result;
	bool found;

	Assert(index >= 0); /* -1 is reserved for not-in-table */

	result = (PandoraRelationMetaLookupEntry *) hash_search_with_hash_value(
						PandoraSharedRelationMetaHash,
						(void *) tagPtr, hashcode, HASH_ENTER, &found);

	if (found) /* found something already in the hash table */
		return result->id;

	result->id = index;

	return -1;
}

/*
 * PandoraRelationMetaHashDelete
 *
 * Delete the hashtable entry for given tag (must exist)
 *
 * Caller must hold exclusive lock on tag's partition
 */
void
PandoraRelationMetaHashDelete(const PandoraRelationMetaTag *tagPtr, 
																uint32 hashcode)
{
	PandoraRelationMetaLookupEntry *result;

	result = (PandoraRelationMetaLookupEntry *) hash_search_with_hash_value(
		PandoraSharedRelationMetaHash, 
		(void *) tagPtr, hashcode, HASH_REMOVE, NULL);

	if (!result) 
		elog(ERROR, "shared relation meta hash table corrupted");
}

#endif /* PANDORA */
