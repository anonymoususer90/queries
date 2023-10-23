/*-------------------------------------------------------------------------
 *
 * pandora_catalog.h
 *	  POSTGRES PANDORA catalogs.
 *
 *
 * src/include/pandora/pandora_catalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PANDORA_CATALOG_H
#define PANDORA_CATALOG_H

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

#include "catalog/pg_pandora.h"

/* common */

extern void PandoraDeleteCatalogTuples(Oid relid);

extern void PandoraRegisterRel(Oid relid,
							   int part_count,
							   DefElem *pandora_spread_factor);


/* pg_pandora */

extern void PandoraInsertRelTuple(Oid relid,
								  int32 part_count,
								  int16 level_count,
								  int16 spread_factor);

extern void PandoraDeleteRelTuple(Oid relid);

extern void PandoraGetMercerTreeInfo(Oid relid,
									 int32 *part_count,
									 int16 *level_count,
									 int16 *spread_factor);

extern int16 PandoraGetSpreadFactor(Oid relid);

extern int16 PandoraGetLevelCount(Oid relid);

extern bool IsPandoraRelId(Oid relid);

extern List *PandoraGetRelOidList(void);

#endif	/* PANDORA_CATALOG_H */
