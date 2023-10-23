/*-------------------------------------------------------------------------
 *
 * pandora_catalog.c
 *	  routines to support PANDORA related catalogs.
 *
 *
 * IDENTIFICATION
 *	  src/backend/pandora/pandora_catalog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/array.h"

#include "pandora/pandora_catalog.h"

/*
 * PandoraDeleteCatalogTuples
 *		Remove PANDORA related catalog tuples.
 *
 * When a table is dropped, all related HAP catalog entries are deleted by this
 * function.
 */
void
PandoraDeleteCatalogTuples(Oid relid)
{
	/* pg_pandora */
	PandoraDeleteRelTuple(relid);
}

/*
 * PandoraUpdateCatalogPartitioningFlag
 *		Update pg_class to record pandora's online-partitioing flag.
 *
 * Record this table uses pandora's online-partitioning flag.
 */
static void
PandoraUpdateCatalogPartitioningFlag(Oid relid)
{
	Form_pg_class rd_rel;
	Relation pg_class;
	HeapTuple tuple;

	/* Get a modifiable copy of the relation's pg_class row */
	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	rd_rel = (Form_pg_class) GETSTRUCT(tuple);

	/* Update the pg_class row */
	rd_rel->relpandorapartitioning = true;
	rd_rel->relam = get_table_am_oid("pandora", false);
	CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);

	/* Done */
	heap_freetuple(tuple);
	table_close(pg_class, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * PandoraRegisterRel
 * 		Initialize pandora catalog to use online-partitioning.
 *
 * If the HAP module wants to use online-partitioning, it must register metadata
 * to the pandora's catalog.
 *
 * Before registering, translate DefElem and get the level info too.
 */
void
PandoraRegisterRel(Oid relid,
				  int part_count, DefElem *spread_factor_def)
{
	int16 level_count = 1, spread_factor = 16; /* default */
	int tmp_part_count = 1;

	if (spread_factor_def)
	{
		if (!IsA(spread_factor_def->arg, Integer))
			elog(ERROR, "Pandora's spread factor must be a int");

		spread_factor = ((Integer *) spread_factor_def->arg)->ival;
	}

	if (spread_factor > part_count)
		spread_factor = part_count;

	/* Calculate mercer tree's level */
	while (tmp_part_count < part_count)
	{
		level_count++;
		tmp_part_count *= spread_factor;
	}

	PandoraInsertRelTuple(relid, part_count, level_count, spread_factor);

	PandoraUpdateCatalogPartitioningFlag(relid);
}
