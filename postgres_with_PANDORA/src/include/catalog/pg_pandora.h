/*-------------------------------------------------------------------------
 *
 * pg_pandora.h
 *	  definition of the "PANDORA" system catalog (pg_pandora)
 *
 *
 * src/include/catalog/pg_hap.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PANDORA_H
#define PG_PANDORA_H

#include "catalog/genbki.h"
#include "catalog/pg_pandora_d.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------
 *		pg_pandora definition.	cpp turns this into
 *		typedef struct FormData_pg_pandora
 * ----------------
 */
CATALOG(pg_pandora,9986,PandoraRelationId)
{
	Oid			pandorarelid		BKI_LOOKUP(pg_class);
	int32		pandorapartcount	BKI_DEFAULT(0);
	int16		pandoralevelcount	BKI_DEFAULT(0);
	int16		pandoraspreadfactor	BKI_DEFAULT(0);
} FormData_pg_pandora;

/* ----------------
 *		Form_pg_pandora corresponds to a pointer to a tuple with
 *		the format of pg_pandora relation.
 * ----------------
 */
typedef FormData_pg_pandora *Form_pg_pandora;

DECLARE_UNIQUE_INDEX_PKEY(pg_pandora_pandorarelid_index,9985,PandoraRelIdIndexId,on pg_pandora using btree(pandorarelid oid_ops));

#endif	/* PG_PANDORA_H */
