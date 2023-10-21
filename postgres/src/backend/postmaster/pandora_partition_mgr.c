/*-------------------------------------------------------------------------
 *
 * partition_mgr.c
 *
 * Partitioning Process Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/partition_mgr.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef PANDORA
#include "postgres.h"

#include <pthread.h>
#include <math.h>
#include "libpq/pqsignal.h"
#include "postmaster/interrupt.h"
#include "postmaster/fork_process.h"
#include "miscadmin.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/pmsignal.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/wait_event.h"
#include "utils/timeout.h"
#include "utils/ps_status.h"
#include "utils/fmgroids.h"
#include "catalog/pg_database.h"
#include "access/sdir.h"
#include "access/xact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/genam.h"
#include "tcop/tcopprot.h"

#include "storage/ebi_tree.h"

#include "postmaster/pandora_partition_mgr.h"
#include "pandora/pandora_partitioning.h"
#include "pandora/pandora_relation_meta.h"
#include "pandora/pandora_catalog.h"

int PartitioningDelay = 1000; /* milli-seconds */

PandoraPartitioningShmemStruct *PartitioningShmem = NULL;

/* Flags to tell if we are in an partition manager process. */
static bool am_pandora_partition_launcher = false;
static bool am_pandora_partition_worker = false;

/* Prototypes for private functions */
static void PandoraHandlePartitionManagerInterrupts(void);
static List *PandoraGetDatabaseList(void);
static HeapTuple PandoraGetDatabaseTupleByOid(Oid dboid);
static bool IsConnectionAllowed(Oid databaseOid);
static void PandoraPartitionDsaAttach(void);
static void PandoraPartitionDsaDetach(void);

/* Memory context for long-lived data */
static MemoryContext PartMgrMemCxt;

static void PandoraHandlePartitionManagerLauncherInterrupts(void);

#ifdef PANDORA_MEASURE
static double 
GetDifferenceInMilliSeconds(struct timespec *begin, struct timespec *end)
{
	double diff = (end->tv_sec - begin->tv_sec) * 1000.0 + 
					(end->tv_nsec - begin->tv_nsec) / 1000000.0;

	return diff;
}
#endif

/*
 * Main entry point for partition manager launcher process, to be called from
 * the postmaster.
 */
int
StartPandoraPartitionLauncher(void)
{
	pid_t PartitionManagerLauncherPID;

	switch ((PartitionManagerLauncherPID = fork_process()))
	{
		case -1:
			ereport(LOG,
			(errmsg("could not fork pandora partition launcher process: %m")));
			return 0;

		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			PandoraPartitionLauncherMain(0, NULL);
			break;
		default:
			return (int) PartitionManagerLauncherPID;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * Perform a normal exit from the pandora partition launcher.
 */
static void
PartitionManagerLauncherShutdown(void)
{
	ereport(DEBUG1,
			(errmsg_internal("partition manager launcher shutting down")));
	PartitioningShmem->launcherpid = 0;

	/* Unset flag and BlockCheckpointerBits for checkpointer */
	PartitioningShmem->isLauncherAlive = false;
	pg_memory_barrier();
	UnsetAllBlockCheckpointerBits();

	/* dsa detach */
	PandoraPartitionDsaDetach();

	proc_exit(0);				/* done */
}

/*
 * Process any new interrupts.
 */
static void
PandoraHandlePartitionManagerLauncherInterrupts(void)
{
	if (ProcSignalBarrierPending) 
		ProcessProcSignalBarrier();

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	if (ShutdownRequestPending)
	{
		PartitionManagerLauncherShutdown();

		/* Partition launcher can access this line. */
		Assert(false);
	}
}

/*
 * Main loop for the pandora partition launcher process.
 */
void
PandoraPartitionLauncherMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	uint64_t dbindex = 0;

	am_pandora_partition_launcher = true;

	MyBackendType = B_PANDORA_PARTITION_LAUNCHER;
	init_ps_display(NULL);

	pg_usleep(1000000L);

	SetProcessingMode(InitProcessing);

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 *
	 * We have no particular use for SIGINT at the moment, but seems
	 * reasonable to treat like SIGTERM.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);

	InitializeTimeouts();

	pqsignal(SIGQUIT, SignalHandlerForCrashExit);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN); /* not used */

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
	InitProcess();

	/* Early initialization */
	BaseInit();

	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, false, false, NULL);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	PartMgrMemCxt = AllocSetContextCreate(TopMemoryContext,
										  "Partition Manager Launcher",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(PartMgrMemCxt);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is a stripped down version of PostgresMain error recovery.
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
	 * signals other than SIGQUIT will be blocked until we complete error
	 * recovery.  It might seem that this policy makes the HOLD_INTERRUPTS()
	 * call redundant, but it is not since InterruptPending might be set
	 * already.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Forget any pending QueryCancel or timeout request */
		disable_all_timeouts(false);
		QueryCancelPending = false; /* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/* Abort the current transaction in order to recover */
		AbortCurrentTransaction();

		/*
		 * Release any other resources, for the case where we were not in a
		 * transaction.
		 */
		LWLockReleaseAll();
		pgstat_report_wait_end();
		AbortBufferIO();
		UnlockBuffers();
		/* this is probably dead code, but let's be safe: */
		if (AuxProcessResourceOwner)
			ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(PartMgrMemCxt);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(PartMgrMemCxt);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/* if in shutdown mode, no need for anything further; just go away */
		if (ShutdownRequestPending)
			PartitionManagerLauncherShutdown();

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* must unblock signals before calling rebuild_database_list */
	PG_SETMASK(&UnBlockSig);

	PartitioningShmem->launcherpid = MyProcPid;
	PartitioningShmem->isLauncherAlive = true;

	/* dsa init */
	PandoraPartitionDsaInit();

	/* loop until shutdown request */
	while (!ShutdownRequestPending)
	{
		List *dbOidList = PandoraGetDatabaseList();
		Oid dbid = list_nth_oid(dbOidList, dbindex % list_length(dbOidList));

		PandoraHandlePartitionManagerLauncherInterrupts();
		ResetLatch(MyLatch);

#if 0
		/*
		 * a worker finished, or postmaster signaled failure to start a worker
		 */
		if (got_SIGUSR2)
		{
			got_SIGUSR2 = false;

			
			if (AutoVacuumShmem->av_signal[AutoVacForkFailed])
			{
				/*
				 * If the postmaster failed to start a new worker, we sleep
				 * for a little while and resend the signal.  The new worker's
				 * state is still in memory, so this is sufficient.  After
				 * that, we restart the main loop.
				 *
				 * XXX should we put a limit to the number of times we retry?
				 * I don't think it makes much sense, because a future start
				 * of a worker will continue to fail in the same way.
				 */
				AutoVacuumShmem->av_signal[AutoVacForkFailed] = false;
				pg_usleep(1000000L);	/* 1s */
				SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER);
				continue;
			}
		}
#endif
		dbindex += 1;

		if (!IsConnectionAllowed(dbid))
			continue;

		/* initializing */
		PartitioningShmem->initDbOid = dbid;
		PartitioningShmem->initializing = true;
		SendPostmasterSignal(PMSIGNAL_PANDORA_PARTITIONING_WORKER);

		/*
		 * Wait until naptime expires or we get some type of signal (all the
		 * signal handlers will wake us by calling SetLatch).
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (PANDORA_PARTITIONING_LAUNCHER_SLEEP_MILLI_SECOND),
						 WAIT_EVENT_PARTITION_MANAGER_MAIN);


		/* Wait intialing end. */
		while (PartitioningShmem->initializing == true)
			sched_yield();

		while (pg_atomic_read_u64(&PartitioningShmem->currentWorkerNums) <
					pg_atomic_read_u64(&PartitioningShmem->neededWorkerNums))
		{
			int oldWorkerNums = 
				pg_atomic_read_u64(&PartitioningShmem->currentWorkerNums);

			/* We're OK to start a new worker */
			SendPostmasterSignal(PMSIGNAL_PANDORA_PARTITIONING_WORKER);

			ResetLatch(MyLatch);

			while (oldWorkerNums == 
					pg_atomic_read_u64(&PartitioningShmem->currentWorkerNums))
			{
				PandoraHandlePartitionManagerLauncherInterrupts();
				/*
				 * Wait until naptime expires or we get some type of signal (all the
				 * signal handlers will wake us by calling SetLatch).
				 */
				(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 (PANDORA_PARTITIONING_LAUNCHER_SLEEP_MILLI_SECOND),
							 WAIT_EVENT_PARTITION_MANAGER_MAIN);
			}

			Assert(oldWorkerNums + 1 == 
					pg_atomic_read_u64(&PartitioningShmem->currentWorkerNums));
		}
	}

	PartitionManagerLauncherShutdown();
}

/*
 * Main entry point for repartitioning process, to be called from the
 * postmaster.
 */
int
StartPandoraPartitionWorker(void)
{
	pid_t	PartitionManagerPID;

	switch ((PartitionManagerPID = fork_process()))
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork partition manager process: %m")));
			return 0;
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			PandoraPartitionWorkerMain(0, NULL);
			break;
		default:
			return (int) PartitionManagerPID;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * ExceedRepartitioningThreshold
 *
 * Before proceeding with repartitioning, we perform a file size check. If the 
 * partition file is smaller than the specified threshold, we return false.
 */
static bool
ExceedRepartitioningThreshold(int recordsCnt, int recordNumsPerBlock)
{
	int pageNums;

	if (recordsCnt == 0)
		pageNums = 0;
	else
		pageNums = ((recordsCnt - 1) / recordNumsPerBlock) + 1;

	if (pageNums < (PANDORA_REPARTITIONING_THRESHOLD / BLCKSZ))
		return false;

	return true;
}

/*
 * PandoraPartitionJobExists
 */
static bool
PandoraPartitionJobExists(Oid dbOid, Oid relOid)
{
	PandoraPartitionJob *job;

	for (int idx = 0; idx < PANDORA_PARTITION_JOB_MAX; ++idx)
	{
		job = &PartitioningShmem->partitionJobs[idx];

		if (job->inited && job->dbOid == dbOid && job->relOid == relOid)
			return true;
	}

	return false;
}

/*
 * PandoraGetPartitionJob
 */
static PandoraPartitionJob *
PandoraGetEmptyPartitionJob(void)
{
	PandoraPartitionJob *job;

	for (int idx = 0; idx < PANDORA_PARTITION_JOB_MAX; ++idx)
	{
		job = &PartitioningShmem->partitionJobs[idx];

		if (job->inited)
			continue;

		if (LWLockConditionalAcquire(&job->partitionJobLock, LW_EXCLUSIVE))
		{
			Assert(job->dbOid == InvalidOid);
			/* Success for getting the lock. */
			return job;
		}
	}

	Assert(false);
	return NULL;
}

/*
 * PandoraGetPartitionJob
 */
static PandoraPartitionJob *
PandoraGetPartitionJob(void)
{
	PandoraPartitionJob *job;

	for (int idx = 0; idx < PANDORA_PARTITION_JOB_MAX; ++idx)
	{
		job = &PartitioningShmem->partitionJobs[idx];

		if (!job->inited)
			continue;

		Assert(job->dbOid != InvalidOid);
		if (LWLockConditionalAcquire(&job->partitionJobLock, LW_EXCLUSIVE))
		{
			/* Success for getting the lock. */
			return job;
		}
	}

	return NULL;
}

/*
 * PandoraAddPartitionJobs
 *
 * Return added partition job number
 */
static void
PandoraAddPartitionJobs(Oid dbOid)
{
	List *relationOidList = PandoraGetRelOidList();
	ListCell *lc;

	/* Iterate all pandora relations. */
	foreach(lc, relationOidList)
	{
		Oid relationOid = lfirst_oid(lc);

		/* If the partition job is already created, skip it. */
		if (PandoraPartitionJobExists(dbOid, relationOid))
			continue;

		for (int i = 0; i < 2; ++i)
		{
			PandoraPartitionJob *job = PandoraGetEmptyPartitionJob();
		
			/* Init partition job. */
			job->dbOid = dbOid;
			job->relOid = relationOid;
			job->levelZeroOneDedicated = (i == 0 ? true : false);
			job->partitioningWorkerPid = InvalidPid;
			job->inited = true;

			LWLockRelease(&job->partitionJobLock);
		}

		pg_atomic_fetch_add_u64(&PartitioningShmem->neededWorkerNums, 2);
	}
}

/*
 * Main entry point for partition worker
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
PandoraPartitionWorkerMain(int argc, char *argv[])
{
	PandoraPartitionJob *partitionJob;
	Oid dbOid;
	Oid	relationOid;
	sigjmp_buf localSigjmpBuf;
	MemoryContext partitioningContext;

	am_pandora_partition_worker = true;

	MyBackendType = B_PANDORA_PARTITION_WORKER;
	init_ps_display(NULL);

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);

	/*
	 * SIGINT is used to signal canceling the current table's vacuum; SIGTERM
	 * means abort and exit cleanly, and SIGQUIT means abandon ship.
	 */
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */

	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	InitProcess();

	BaseInit();

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	partitioningContext = AllocSetContextCreate(
		TopMemoryContext, "PANDORA PARTITION WORKER", ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(partitioningContext);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * Unlike most auxiliary processes, we don't attempt to continue
	 * processing after an error; we just clean up and exit.
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
	 * signals other than SIGQUIT will be blocked until we exit.  It might
	 * seem that this policy makes the HOLD_INTERRUPTS() call redundant, but
	 * it is not since InterruptPending might be set already.
	 */
	if (sigsetjmp(localSigjmpBuf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.  Note that because we called InitProcess, a
		 * callback was registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}


	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &localSigjmpBuf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Advertise our latch that backends can use to wake us up while we're
	 * sleeping.
	 */
	ProcGlobal->partitionmanagerLatch = &MyProc->procLatch;

	/* dsa init */
	PandoraPartitionDsaInit();

	/*
	 * The worker just sets partition jobs for others in here.
	 */
	if (PartitioningShmem->initializing)
	{
		InitPostgres(NULL, PartitioningShmem->initDbOid, 
									NULL, InvalidOid, false, false, NULL);

		/* Start a transaction so our commands have one to play into. */
		StartTransactionCommand();

		/* Add new partition jobs. */
		PandoraAddPartitionJobs(PartitioningShmem->initDbOid);
		pg_memory_barrier();

		PartitioningShmem->initDbOid = InvalidOid;
		PartitioningShmem->initializing = false;
		CommitTransactionCommand();

		proc_exit(0);
		pg_unreachable();
	}

	/*
	 * Partitioning entry.
	 */

	/* Get partition job. */
	partitionJob = PandoraGetPartitionJob();
	if (partitionJob == NULL)
		elog(ERROR, "fail to get partition job for worker.");
	partitionJob->partitioningWorkerPid = MyProcPid;

	/* Get target database oid and relation oid. */
	dbOid = partitionJob->dbOid;
	relationOid = partitionJob->relOid;
	InitPostgres(NULL, dbOid, NULL, InvalidOid, false, false, NULL);
	
	pg_atomic_fetch_add_u64(&PartitioningShmem->currentWorkerNums, 1);

	/* 
	 * Loop forever
	 */
	for (;;)
	{
		int	partitionCompleteNums = 0;
		PandoraRelationMeta *meta;
		PandoraRepartitioningDesc desc;
		pg_atomic_uint64 *parent_refcounter;
		pg_atomic_uint64 *child_refcounters;
		PandoraPartLevel	partitionLevel; 
		PandoraPartLevel	firstTargetPartitionLevel, lastTargetPartitionLevel;
		uint64				tieredRecordCount;
		uint32				reorganizedRecCnt[32];
		PandoraSeqNum		firstSeqNums[32];
		dsa_pointer			dsaPartitionDeltaSpaceEntry;
		TransactionId xmin;
		TransactionId xmax;

		/* Start a transaction so our commands have one to play into. */
		StartTransactionCommand();

		/* Get relation meta. */
		meta = PandoraGetRelationMeta(relationOid);		

		PandoraHandlePartitionManagerInterrupts();
		ResetLatch(MyLatch);
		
		/* Init values for repartitioning descriptior. */
		desc.meta = meta;

		/* Increment count of partitioning worker */
		IncrPartitioningWorkerCount(meta);

		/* Partition worker has domain for partitioning. */
		if (partitionJob->levelZeroOneDedicated)
		{
			LWLock *metaMempartitionLock =
						PandoraRelationMetaMemPartitionLock(meta);
			PandoraPartNumber minPartitionNumber;
			PandoraPartNumber maxPartitionNumber;

			/* Do partition on level zero. */
			partitionLevel = 0;
					
#ifdef PANDORA_MEASURE
			clock_gettime(CLOCK_MONOTONIC, &desc.startTime);
#endif

			LWLockAcquire(metaMempartitionLock, LW_SHARED);

			minPartitionNumber = meta->partitionNumberLevelZero[0];
			maxPartitionNumber = meta->partitionNumberLevelZero[1];
			tieredRecordCount = meta->recordCountsLevelZero[1];

			LWLockRelease(metaMempartitionLock);

			/* If there are no tiered records, skip */
			if (tieredRecordCount == 0)
			{
				Assert(minPartitionNumber == maxPartitionNumber);
				goto pandora_worker_level_zero_skip;
			}

			/* Set refcounters */
			parent_refcounter =
				PandoraRelationMetaRefCounter(meta, 0, 0);
			child_refcounters =
				PandoraRelationMetaRefCounter(meta, 1, 0);

			MemSet(reorganizedRecCnt, 0, sizeof(uint32) * 32);
			MemSet(firstSeqNums, -1, sizeof(PandoraSeqNum) * 32);

			/* dsa for delta setup. */
			Assert(meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne 
														== InvalidDsaPointer);
			meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne =
						CreatePandoraDeltaSpaceEntry(pandoraPartitionDsaArea);
				
			meta->partitioningLevel = partitionLevel;
			meta->partitioningMinNumber = minPartitionNumber;
			meta->partitioningMaxNumber = maxPartitionNumber - 1;
			pg_memory_barrier();

			/*
			 * Before repartitioning start, we set repartitioning 
			 * inprogress flag.
			 */
			SetRepartitioningStatusInprogress(parent_refcounter);
#ifdef PANDORA_MEASURE
			clock_gettime(CLOCK_MONOTONIC, &desc.copyStartTime);
#endif

			/* Pandora repartitioning main entry. */
			PandoraRepartitioning(&desc, relationOid,
								  tieredRecordCount,
								  partitionLevel, 
								  minPartitionNumber, 
								  maxPartitionNumber - 1,
								  partitionLevel + 1,
								  reorganizedRecCnt,
								  firstSeqNums,
								  true);
					
			pg_memory_barrier();

			LWLockAcquire(metaMempartitionLock, LW_EXCLUSIVE);

			/* Set metadata bit of refcounters */
			SetMetaDataBit(parent_refcounter);
			for (int i = 0; i < meta->spreadFactor; i++)
				SetMetaDataBit(&(child_refcounters[i]));

			/* level 0 */
			meta->partitionNumberLevelZero[0] = maxPartitionNumber;
			meta->recordCountsLevelZero[2] += tieredRecordCount;
			meta->recordCountsLevelZero[1] -= tieredRecordCount;

			/* level 1 */
			for (int i = 0; i < meta->spreadFactor; i++)
			{
				/*
				 * If there are new records that have been
				 * repartitioned, apply that information to the relation
				 * metadata.
				 */
				if (reorganizedRecCnt[i] != 0)
				{
					/*
					 * If these repartitioned records are the first of
					 * this partition
					 */
					if (meta->realRecordsCount[1][i] == 0)
					{
						if (likely(meta->lastPartitionLevel != 1))
						{
							/* Increment generation number if needed */
							if (unlikely(++(meta->generationNums[1][i]) == InvalidPandoraPartGenNo))
								meta->generationNums[1][i] = 0;

							/* Set first sequence number of partition */
							meta->locationInfo[1][i].firstSequenceNumber = firstSeqNums[i];
						}
					}

					/* Adds memoized reorganized records count */
					meta->realRecordsCount[1][i] += reorganizedRecCnt[i];
				}
			}

#ifdef PANDORA_DEBUG
			Assert((pg_atomic_read_u64(meta->nblocks) + (desc.childBlocks - desc.parentBlocks)) < 0x8000000000000000ULL);
#endif /* PANDORA_DEBUG */

			/* Increment block count for analyze */
			IncrBlockCount(meta, desc.childBlocks - desc.parentBlocks);

			dsaPartitionDeltaSpaceEntry =
				meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne;
			meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne = InvalidDsaPointer;

			meta->partitioningLevel = InvalidPandoraPartLevel;
			meta->partitioningMinNumber = InvalidPandoraPartNumber;
			meta->partitioningMaxNumber = InvalidPandoraPartNumber;

			/*
			 * Repartitioning is done, so we unset all flags.
			 */
			SetRepartitioningStatusDone(parent_refcounter);
			for (int i = 0; i < meta->spreadFactor; i++)
				UnsetMetaDataBit(&(child_refcounters[i]));

			/* Release locks */
			LWLockRelease(metaMempartitionLock);

			/* dsa for delta space cleanup. */
			DeletePandoraDeltaSpaceEntry(pandoraPartitionDsaArea, 
											 dsaPartitionDeltaSpaceEntry);
#ifdef PANDORA_MEASURE
			clock_gettime(CLOCK_MONOTONIC, &desc.endTime);
			elog(LOG, 
					"[PANDORA MEASURE] level: %d, wait(normal): %.3f, memcpy: %.3f, wait(delta): %.3f, applying: %.3f",
					partitionLevel, 
					GetDifferenceInMilliSeconds(&desc.startTime, &desc.copyStartTime),
					GetDifferenceInMilliSeconds(&desc.copyStartTime, &desc.copyEndTime),
					GetDifferenceInMilliSeconds(&desc.copyEndTime, &desc.blockingStartTime),
					GetDifferenceInMilliSeconds(&desc.blockingStartTime, &desc.endTime));
#endif
			pg_memory_barrier();

			/* GC old file: set lifetime of old file */
			xmin = meta->partitionXmax[0][0];	/* previous xmax */
			xmax = EbiGetMaxTransactionId();	/* current xmax */

			/* Link old partition file to ebi node */
			EbiSiftAndBindPartitionOldFile(dbOid, relationOid,
											   xmin,xmax, 0,
											   minPartitionNumber,
											   maxPartitionNumber - 1, 0);

			/* Set new xmax for next repartitioning */
			meta->partitionXmax[0][0] = xmax;

			/* Increment partition complete number. */
			partitionCompleteNums += 1;
		}

pandora_worker_level_zero_skip:
		/* Set target partition range. */
		if (partitionJob->levelZeroOneDedicated)
		{
			firstTargetPartitionLevel = 1;
			lastTargetPartitionLevel = 1;
		}
		else
		{
			firstTargetPartitionLevel = 2;
			lastTargetPartitionLevel = meta->lastPartitionLevel - 1;
		}

		/* Iterate partition levels from 1 to n */
		for (int level = firstTargetPartitionLevel; 
					level <= lastTargetPartitionLevel; level++)	
		{
			PandoraPartLevel childLevel = level + 1;
			PandoraPartNumber childPart;
			int currentPartitionNums = (int) 
							pow(meta->spreadFactor, level);

			/* Set target partition level. */
			partitionLevel = level;

			/* Iteratea all partition numbers. */
			for (int partitionNumber = 0; 
					partitionNumber < currentPartitionNums; 
					++partitionNumber)
			{
				LWLock *levelOnePartitioningLock = NULL;
				int recordsCnt = 
					meta->realRecordsCount[partitionLevel][partitionNumber]; 		
			
				/*
				 * We do not perform repartitioning with a non-existent
				 * file. Also, we calculate size of the partition file.
				 * If the partition file is smaller than the threshold, 
				 * we do not perform partitioning. 
				 */
				if (recordsCnt == 0 || 
						!ExceedRepartitioningThreshold(recordsCnt, 
								meta->recordNumsPerBlock))
					continue;

				/* 
				 * We control the contention between partition workers on same 
				 * relation. 
				 */
				if (partitionJob->levelZeroOneDedicated)
				{
					Assert(partitionLevel == 1);
					/* For workers level 1 and lower. */
					levelOnePartitioningLock = 
						(LWLock*)(&meta->levelOnePartitioningLock[partitionNumber]);
					Assert(partitionNumber < meta->spreadFactor);
				}
				else if (!partitionJob->levelZeroOneDedicated && 
														partitionLevel == 2)
				{
					PandoraPartNumber levelOnePartitionNumber = 
										partitionNumber / meta->spreadFactor;
					
					/* For worker level 2 and above. */
					levelOnePartitioningLock = 
						(LWLock*)(&meta->levelOnePartitioningLock[levelOnePartitionNumber]);
					Assert(levelOnePartitionNumber <= meta->spreadFactor);
				}

				/* Try to get a lock. */
				if (levelOnePartitioningLock && 
						!LWLockConditionalAcquire(levelOnePartitioningLock, 
																LW_EXCLUSIVE))
				{
						continue;
				}

				PandoraHandlePartitionManagerInterrupts();
#ifdef PANDORA_MEASURE
				clock_gettime(CLOCK_MONOTONIC, &desc.startTime);
#endif

				/* Number of first partition */
				childPart = partitionNumber * meta->spreadFactor;

				/* Set refcounters */
				parent_refcounter =
					PandoraRelationMetaRefCounter(meta, partitionLevel,
													  partitionNumber);
				child_refcounters =
					PandoraRelationMetaRefCounter(meta, childLevel,
													  childPart);

				MemSet(reorganizedRecCnt, 0, sizeof(uint32) * 32);
				MemSet(firstSeqNums, -1, sizeof(PandoraSeqNum) * 32);

				/* dsa for delta setup. */
				if (partitionLevel <= 1)
				{
					Assert(meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne == 
														InvalidDsaPointer);
					meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne =
						CreatePandoraDeltaSpaceEntry(pandoraPartitionDsaArea);
				}
				else
				{
					Assert(meta->dsaPartitionDeltaSpaceEntryForOthers == 
														InvalidDsaPointer);
					meta->dsaPartitionDeltaSpaceEntryForOthers =
						CreatePandoraDeltaSpaceEntry(pandoraPartitionDsaArea);
				}
				pg_memory_barrier();

				/*
				 * Before repartitioning start, we set repartitioning 
				 * inprogress flag.
				 */
				SetRepartitioningStatusInprogress(parent_refcounter);

#ifdef PANDORA_MEASURE
				clock_gettime(CLOCK_MONOTONIC, &desc.copyStartTime);
#endif

				/* Pandora repartitioning main entry. */
				PandoraRepartitioning(&desc, relationOid,
									  recordsCnt,
									  partitionLevel, 
									  partitionNumber, 
									  partitionNumber,
									  childLevel,
									  reorganizedRecCnt,
									  firstSeqNums,
									  true);
				
				pg_memory_barrier();

				/* Set metadata bit of refcounters */
				SetMetaDataBit(parent_refcounter);
				for (int i = 0; i < meta->spreadFactor; i++)
					SetMetaDataBit(&(child_refcounters[i]));

				/* parent */
				meta->locationInfo[partitionLevel][partitionNumber].reorganizedRecordsCount +=
						meta->realRecordsCount[partitionLevel][partitionNumber];
				meta->realRecordsCount[partitionLevel][partitionNumber] = 0;
					meta->locationInfo[partitionLevel][partitionNumber].firstSequenceNumber = 0xFFFFFFFFFF;

				/* children */
				for (int i = 0, child_i = childPart;
					 i < meta->spreadFactor; i++, child_i++)
				{
					/*
					 * If there are new records that have been
					 * repartitioned, apply that information to the
					 * relation metadata.
					 */
					if (reorganizedRecCnt[i] != 0)
					{
						/*
						 * If these repartitioned records are the first
						 * of this partition
						 */
						if (meta->realRecordsCount[childLevel][child_i] == 0)
						{
							if (likely(childLevel != meta->lastPartitionLevel))
							{
								/* Increment generation number if needed */
								if (unlikely(++(meta->generationNums[childLevel][child_i]) == InvalidPandoraPartGenNo))
									meta->generationNums[childLevel][child_i] = 0;

								/* Set first sequence number of partition */
								meta->locationInfo[childLevel][child_i].firstSequenceNumber = firstSeqNums[i];
							}
						}
						
						/* Adds memoized reorganized records count */
						meta->realRecordsCount[childLevel][child_i] += reorganizedRecCnt[i];
					}
				}
#ifdef PANDORA_DEBUG
				Assert((pg_atomic_read_u64(meta->nblocks) + (desc.childBlocks - desc.parentBlocks)) < 0x8000000000000000ULL);
#endif /* PANDORA_DEBUG */

				/* Increment block count for analyze */
				IncrBlockCount(meta, desc.childBlocks - desc.parentBlocks);
				
				/* Remove dsa area for delta space. */
				if (partitionLevel <= 1)
				{
					Assert(meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne != 
														InvalidDsaPointer);
					dsaPartitionDeltaSpaceEntry = 
						meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne;
					meta->dsaPartitionDeltaSpaceEntryForLevelZeroOne = 
															InvalidDsaPointer;
				}
				else
				{
					Assert(meta->dsaPartitionDeltaSpaceEntryForOthers != 
														InvalidDsaPointer);
					dsaPartitionDeltaSpaceEntry = 
						meta->dsaPartitionDeltaSpaceEntryForOthers;
					meta->dsaPartitionDeltaSpaceEntryForOthers = 
															InvalidDsaPointer;
				}

				/*
				 * Repartitioning is done, so we unset all flags.
				 */
				SetRepartitioningStatusDone(parent_refcounter);
				for (int i = 0; i < meta->spreadFactor; i++)
					UnsetMetaDataBit(&(child_refcounters[i]));

				/* dsa for delta space cleanup. */
				DeletePandoraDeltaSpaceEntry(pandoraPartitionDsaArea, 
												 dsaPartitionDeltaSpaceEntry);
#ifdef PANDORA_MEASURE
				clock_gettime(CLOCK_MONOTONIC, &desc.endTime);
				elog(LOG, 
						"[PANDORA MEASURE] level: %d, wait(normal): %.3f, memcpy: %.3f, wait(delta): %.3f, applying: %.3f",
						partitionLevel, 
						GetDifferenceInMilliSeconds(&desc.startTime, &desc.copyStartTime),
						GetDifferenceInMilliSeconds(&desc.copyStartTime, &desc.copyEndTime),
						GetDifferenceInMilliSeconds(&desc.copyEndTime, &desc.blockingStartTime),
						GetDifferenceInMilliSeconds(&desc.blockingStartTime, &desc.endTime));
#endif
				pg_memory_barrier();

				/* GC old file: set lifetime of old file */
				xmin = meta->partitionXmax[partitionLevel][partitionNumber];	/* previous xmax */
				xmax = EbiGetMaxTransactionId();	/* current xmax */

				/* Link old partition file to ebi node */
				EbiSiftAndBindPartitionOldFile(dbOid, relationOid,
											   xmin,xmax, partitionLevel,
											   partitionNumber,
											   partitionNumber,
											   meta->generationNums[partitionLevel][partitionNumber]);

				/* Set new xmax for next repartitioning */
				meta->partitionXmax[partitionLevel][partitionNumber] = xmax;

				if (levelOnePartitioningLock)
					LWLockRelease(levelOnePartitioningLock);

				/* Increment partition complete number. */
				partitionCompleteNums += 1;
			}
		}
		

		/* Decrement count of partitioning worker */
		DecrPartitioningWorkerCount(meta);

		pg_memory_barrier();

		/* If launcher was terminated, notify it to checkpointer */
		if (unlikely(!PartitioningShmem->isLauncherAlive && IsNoWorker(meta)))
			UnsetBlockCheckpointerBit(meta);
	
		CommitTransactionCommand();	

		if (partitionCompleteNums == 0)
		{
			/* 
			 * If the worker doesn't complete the partitioning job, it will wait 
			 * for a short time. 
			 */
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 (PANDORA_PARTITIONING_WORKER_SLEEP_MILLI_SECOND),
							 WAIT_EVENT_PARTITION_MANAGER_MAIN);
		}
	}

	LWLockRelease(&partitionJob->partitionJobLock);
	pg_atomic_fetch_sub_u64(&PartitioningShmem->currentWorkerNums, 1);

	proc_exit(0);
}

/*
 * IsAutoVacuumLauncherProcess
 */
bool
IsPandoraPartitionManagerProcess(void)
{
	return am_pandora_partition_worker;
}

/*
 * Is this process pandora partitioning launcher?
 */
bool
IsPandoraPartitionLauncherProcess(void)
{
	return am_pandora_partition_launcher;
}

/*
 * Process any new interrupts.
 */
static void
PandoraHandlePartitionManagerInterrupts(void)
{
	if (ProcSignalBarrierPending) 
		ProcessProcSignalBarrier();

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	if (ShutdownRequestPending)
	{
		/* Normal exit from the Partition Manger is here */
		proc_exit(0); /* done */
	}
}

/* --------------------------------
 *		communication with backends
 * --------------------------------
 */

/*
 * PartitioningShmemSize
 *		Compute space needed for partitioning related shared memory
 */
Size
PandoraPartitioningShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(PandoraPartitioningShmemStruct));

	return size;
}

/*
 * PartitioningShmemInit
 *		Allocate and initialize EBI tree related shared memory
 */
void
PandoraPartitioningShmemInit(void)
{
	Size size = PandoraPartitioningShmemSize();
	bool found;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	PartitioningShmem =
		(PandoraPartitioningShmemStruct *) ShmemInitStruct(
				"Partitioning Data", sizeof(PandoraPartitioningShmemStruct), &found);

	if (!found)
	{
		/*
		 * First time through, so initialize.
		 */
		MemSet(PartitioningShmem, 0, size);
		Assert(PartitioningShmem != NULL);

		PartitioningShmem->initDbOid = InvalidOid;
		PartitioningShmem->initializing = false;
		PartitioningShmem->isLauncherAlive = false;
		pg_atomic_init_u64(&PartitioningShmem->currentWorkerNums, 0);
		pg_atomic_init_u64(&PartitioningShmem->neededWorkerNums, 0);

		for (int idx = 0; idx < PANDORA_PARTITION_JOB_MAX; ++idx)
		{
			PandoraPartitionJob *job = &PartitioningShmem->partitionJobs[idx];

			job->partitioningWorkerPid = InvalidPid;
			job->inited = false;
			LWLockInitialize(&job->partitionJobLock, LWTRANCHE_PANDORA_PARTITION);
		}
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * get_database_list
 *		Return a list of all databases found in pg_database.
 *
 * The list and associated data is allocated in the caller's memory context,
 * which is in charge of ensuring that it's properly cleaned up afterwards.
 */
static List *
PandoraGetDatabaseList(void)
{
	List	   *dblist = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	/*
	 * Start a transaction so we can access pg_database, and get a snapshot.
	 * We don't have a use for the snapshot itself, but we're interested in
	 * the secondary effect that it sets RecentGlobalXmin.  (This is critical
	 * for anything that reads heap pages, because HOT may decide to prune
	 * them even if the process doesn't attempt to modify any tuples.)
	 *
	 * FIXME: This comment is inaccurate / the code buggy. A snapshot that is
	 * not pushed/active does not reliably prevent HOT pruning (->xmin could
	 * e.g. be cleared when cache invalidations are processed).
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		MemoryContext oldcxt;

		/*
		 * Allocate our results in the caller's context, not the
		 * transaction's. We do this inside the loop, and restore the original
		 * context at the end, so that leaky things like heap_getnext() are
		 * not called in a potentially long-lived context.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);


		dblist = lappend_oid(dblist, pgdatabase->oid);
		MemoryContextSwitchTo(oldcxt);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	/* Be sure to restore caller's memory context */
	MemoryContextSwitchTo(resultcxt);

	return dblist;
}

/*
 * PandoraGetDatabaseTupleByOid -- as above, but search by database OID
 */
static HeapTuple
PandoraGetDatabaseTupleByOid(Oid dboid)
{
	HeapTuple	tuple;
	Relation	relation;
	SysScanDesc scan;
	ScanKeyData key[1];

	/*
	 * form a scan key
	 */
	ScanKeyInit(&key[0],
				Anum_pg_database_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dboid));

	/*
	 * Open pg_database and fetch a tuple.  Force heap scan if we haven't yet
	 * built the critical shared relcache entries (i.e., we're starting up
	 * without a shared relcache cache file).
	 */
	relation = table_open(DatabaseRelationId, AccessShareLock);
	scan = systable_beginscan(relation, DatabaseOidIndexId,
							  criticalSharedRelcachesBuilt,
							  NULL,
							  1, key);

	tuple = systable_getnext(scan);

	/* Must copy tuple before releasing buffer */
	if (HeapTupleIsValid(tuple))
		tuple = heap_copytuple(tuple);

	/* all done */
	systable_endscan(scan);
	table_close(relation, AccessShareLock);

	return tuple;
}

/*
 * IsConnectionAllowed
 */
static bool
IsConnectionAllowed(Oid databaseOid)
{
	HeapTuple	tup;
	Form_pg_database dbform;
	bool allowConnection;

	StartTransactionCommand();

	/* Fetch our pg_database row normally, via syscache */
	tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(databaseOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for database %u", databaseOid);
	dbform = (Form_pg_database) GETSTRUCT(tup);

	allowConnection = dbform->datallowconn;
	ReleaseSysCache(tup);

	CommitTransactionCommand();
	return allowConnection;
}

/* --------------------------------
 *		pandora partition dsa
 * --------------------------------
 */

/*
 * PandoraPartitionDsaInit
 */
void
PandoraPartitionDsaInit(void)
{
	/* This process is selected to create dsa_area itself */
	MemoryContext oldMemoryContext;

	oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
	/*
	 * The first backend process creates the dsa area for EBI tree,
	 * and another backend processes waits the creation and then attach to it.
	 */
	if (PartitioningShmem->pandoraPartitionHandle == 0)
	{
		uint32 expected = 0;
		if (pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)
				(&PartitioningShmem->pandoraPartitionHandle),
					&expected, UINT32_MAX))
		{
			dsa_area *area;
			dsa_handle handle;

			/* Initialize dsa area for vcluster */
			area = dsa_create(LWTRANCHE_PANDORA_PARTITION);
			handle = dsa_get_handle(area);

			dsa_pin(area);
			dsa_detach(area);

			pg_memory_barrier();

			PartitioningShmem->pandoraPartitionHandle = handle;
		}
	}

	while (pg_atomic_read_u32((pg_atomic_uint32 *) 
				&PartitioningShmem->pandoraPartitionHandle) == UINT32_MAX)
	{
		/*
		 * Another process is creating an initial dsa area for EBI tree,
		 * so just wait it to finish and then attach to it.
		 */
		pg_usleep(1000 * 1000);
	}

	PandoraPartitionDsaAttach();

	MemoryContextSwitchTo(oldMemoryContext);
}

/*
 * PandoraPartitionDsaAttach
 */
static void
PandoraPartitionDsaAttach(void)
{
	if (pandoraPartitionDsaArea != NULL) return;

	pandoraPartitionDsaArea = 
		dsa_attach(PartitioningShmem->pandoraPartitionHandle);
	dsa_pin_mapping(pandoraPartitionDsaArea);
}

/*
 * PandoraPartitionDsaDetach
 */
static void
PandoraPartitionDsaDetach(void)
{
	if (pandoraPartitionDsaArea == NULL) return;

	dsa_detach(pandoraPartitionDsaArea);
	pandoraPartitionDsaArea = NULL;
}
#endif /* PANDORA */
