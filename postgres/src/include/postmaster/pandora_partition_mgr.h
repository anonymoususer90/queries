/*-------------------------------------------------------------------------
 *
 * pandora_partition_process.h
 *	  partitioning process of PANDORA
 *
 * src/include/postmaster/pandora_partition_process.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PANDORA_PARTITION_PROCESS_H
#define PANDORA_PARTITION_PROCESS_H

#include "c.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"

#define PANDORA_PARTITION_JOB_MAX (32)
#define PANDORA_PARTITIONING_LAUNCHER_SLEEP_MILLI_SECOND (100)
#define PANDORA_PARTITIONING_WORKER_SLEEP_MILLI_SECOND (10)
#define PANDORA_REPARTITIONING_THRESHOLD (16 * 1024 * 1024) /* 16 MiB */

typedef struct PandoraPartitionJob
{
	Oid dbOid;						/* target db oid */
	Oid relOid;						/* target relation oid */
	bool levelZeroOneDedicated;		/* is dedicated from level 0 to level 1? */
	bool inited;

	/* state of partition job */
	volatile pid_t partitioningWorkerPid;	/* partitioning worker pid */
	LWLock partitionJobLock;
} PandoraPartitionJob;

typedef struct PandoraPartitioningShmemStruct
{
	/* launcher PID and worker PID. */
	pid_t launcherpid;
	bool isLauncherAlive;			/* To handle conflict with checkpointer */

	volatile bool initializing;
	volatile Oid initDbOid;

	PandoraPartitionJob partitionJobs[PANDORA_PARTITION_JOB_MAX];
	pg_atomic_uint64 currentWorkerNums;
	pg_atomic_uint64 neededWorkerNums;

	dsa_handle pandoraPartitionHandle;		/* dsa handle */
} PandoraPartitioningShmemStruct;

extern PandoraPartitioningShmemStruct* PartitioningShmem;

/* src/backend/pandora/paritioning/pandora_paritioning.c */
extern dsa_area* pandoraPartitionDsaArea;
 
extern int StartPandoraPartitionWorker(void);
extern void PandoraPartitionWorkerMain(int argc, char *argv[]);
extern bool IsPandoraPartitionManagerProcess(void);
extern void PandoraPartitioningDsaInit(void);
extern Size PandoraPartitioningShmemSize(void);
extern void PandoraPartitioningShmemInit(void);

extern int StartPandoraPartitionLauncher(void);
extern void PandoraPartitionLauncherMain(int argc, char *argv[]);
extern bool IsPandoraPartitionLauncherProcess(void);
extern void PandoraPartitionDsaInit(void);

#endif /* PANDORA_PARTITION_PROCESS_H */
