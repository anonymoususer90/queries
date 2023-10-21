/*-------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heapam.h
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA

#ifndef EBIAM_H
#define EBIAM_H

#ifdef PANDORA
#include <liburing.h>
#endif /* PANDORA */
#include "nodes/nodes.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "storage/block.h"
#include "access/sdir.h"
#include "executor/tuptable.h"

#ifdef PANDORA
#define INIT_HEAPSCAN_PREFETCH_SIZE (256)
/* TODO: we don't adjust prefech size yet */
#define MAX_HEAPSCAN_PREFETCH_SIZE (256)

StaticAssertDecl(INIT_HEAPSCAN_PREFETCH_SIZE <= MAX_HEAPSCAN_PREFETCH_SIZE ,
				"heapscan prefetch size setting");
#endif /* PANDORA */

/* Tricky variable for passing the cmd type from ExecutePlan to heap code */
extern CmdType curr_cmdtype;

extern void ebigetpage(HeapScanDesc scan);
extern bool ebi_getnextslot(TableScanDesc sscan, ScanDirection direction, 
								struct TupleTableSlot *slot);
#ifdef PANDORA
extern void InsertUnobtainedVersionOffset(HeapScanDesc scan, 
										  EbiSubVersionOffset versionOffset);
#endif /* PANDORA */

#endif							/* EBIAM_H */
#endif							/* DIVA */
