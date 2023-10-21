/*-------------------------------------------------------------------------
 *
 * pg_pandora.c
 *	  routines to support manipulation of the pg_pandora catalog
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/pg_hap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_hap.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "pandora/pandora_catalog.h"

/*
 * PandoraInsertRelTuple
 *		Create a single pg_pandora entry for the new relation.
 *
 * When a table uses PANDORA as partitioning method, pg_pandora entry is
 * created.
 */
void
PandoraInsertRelTuple(Oid relid, int32 part_count,
					  int16 level_count, int16 spread_factor)
{
	Datum		values[Natts_pg_pandora];
	bool		nulls[Natts_pg_pandora];
	HeapTuple	htup;
	Relation	pgpandoraRel;

	pgpandoraRel = table_open(PandoraRelationId, RowExclusiveLock);

	/* Make a pg_pandora entry */
	values[Anum_pg_pandora_pandorarelid - 1]
		= ObjectIdGetDatum(relid);
	values[Anum_pg_pandora_pandorapartcount - 1]
		= Int32GetDatum(part_count);
	values[Anum_pg_pandora_pandoralevelcount - 1]
		= Int16GetDatum(level_count);
	values[Anum_pg_pandora_pandoraspreadfactor - 1]
		= Int16GetDatum(spread_factor);

	/* There is no nullable column */
	memset(nulls, 0, sizeof(nulls));

	htup = heap_form_tuple(RelationGetDescr(pgpandoraRel), values, nulls);

	CatalogTupleInsert(pgpandoraRel, htup);

	heap_freetuple(htup);

	table_close(pgpandoraRel, RowExclusiveLock);
}

/*
 * Delete a single pg_pandora entry for the dropped relation.
 */
void
PandoraDeleteRelTuple(Oid relId)
{
	Relation	pgpandoraRel;
	ScanKeyData	scanKey;
	SysScanDesc	scanDesc;
	HeapTuple	htup;

	/* Find and delete the pg_hap entry */
	pgpandoraRel = table_open(PandoraRelationId, RowExclusiveLock);
	ScanKeyInit(&scanKey,
				Anum_pg_pandora_pandorarelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relId));
	scanDesc = systable_beginscan(pgpandoraRel, PandoraRelIdIndexId,
								  true, NULL, 1, &scanKey);

	/* relId is uniqure on the pg_hap, so loop is unnecessary */
	if (HeapTupleIsValid(htup = systable_getnext(scanDesc)))
		CatalogTupleDelete(pgpandoraRel, &htup->t_self);

	/* Done */
	systable_endscan(scanDesc);
	table_close(pgpandoraRel, RowExclusiveLock);
}

/*
 * PandoraGetMercerTreeInfo
 *		Get mercer tree's metadata
 *
 * Lookup the pg_pandora_pandorarelid_index using relation oid.
 * Save metadatas to the arguments.
 */
void
PandoraGetMercerTreeInfo(Oid relid, int32 *part_count,
						 int16 *level_count, int16 *spread_factor)
{
	Form_pg_pandora pandoratup;
	HeapTuple tuple;

	tuple = SearchSysCache1(PANDORARELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
	{
		*part_count = 0;
		*level_count = 0;
		*spread_factor = 0;
		return;
	}

	pandoratup = (Form_pg_pandora) GETSTRUCT(tuple);
	*part_count = pandoratup->pandorapartcount;
	*level_count = pandoratup->pandoralevelcount;
	*spread_factor = pandoratup->pandoraspreadfactor;

	ReleaseSysCache(tuple);
}

/*
 * Return spread factor.
 */
int16
PandoraGetSpreadFactor(Oid relid)
{
	Form_pg_pandora pandora;
	HeapTuple tuple;
	int16 ret;

	tuple = SearchSysCache1(PANDORARELID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for pandora spread factor");

	pandora = (Form_pg_pandora) GETSTRUCT(tuple);
	ret = pandora->pandoraspreadfactor;
	ReleaseSysCache(tuple);

	return ret;
}

/*
 * Return level count.
 */
int16
PandoraGetLevelCount(Oid relid)
{
	Form_pg_pandora pandora;
	HeapTuple tuple;
	int16 ret;

	tuple = SearchSysCache1(PANDORARELID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for pandora spread factor");

	pandora = (Form_pg_pandora) GETSTRUCT(tuple);
	ret = pandora->pandoralevelcount;
	ReleaseSysCache(tuple);

	return ret;
}

/*
 * Check whether the given relation uses PANDORA or not.
 */
bool
IsPandoraRelId(Oid relid)
{
	HeapTuple tuple;

	tuple = SearchSysCache1(PANDORARELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false;

	ReleaseSysCache(tuple);

	return true;
}

/*
 * Return PANDORA relation oid list.
 */
List *
PandoraGetRelOidList(void)
{
	List *ret = NIL;
	HeapTuple tuple;
	SysScanDesc scan;
	Relation pandoraRel;

	pandoraRel = table_open(PandoraRelationId, AccessShareLock);

	scan = systable_beginscan(pandoraRel, InvalidOid, false,
							  NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_pandora pandora = (Form_pg_pandora) GETSTRUCT(tuple);

		ret = lappend_oid(ret, pandora->pandorarelid);
	}

	systable_endscan(scan);
	table_close(pandoraRel, AccessShareLock);

	return ret;
}
