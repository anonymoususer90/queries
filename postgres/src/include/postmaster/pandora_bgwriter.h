/*-------------------------------------------------------------------------
 *
 * pandora_bgwriter.h
 *	  postmaster/pandora_bgwriter.c
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * src/include/postmaster/bgwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PANDORA_BGWRITER_H
#define _PANDORA_BGWRITER_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* GUC options */
//extern PGDLLIMPORT int BgWriterDelay;
extern PGDLLIMPORT int PandoraBgWriterDelay;

//extern void BackgroundWriterMain(void) pg_attribute_noreturn();
extern void PandoraBackgroundWriterMain(void) pg_attribute_noreturn();

//extern Size CheckpointerShmemSize(void);
//extern void CheckpointerShmemInit(void);

#endif /* _PANDORA_BGWRITER_H */
