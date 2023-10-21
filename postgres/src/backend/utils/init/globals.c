/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/file_perm.h"
#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"

#ifdef PANDORA
#include "pandora/pandora_mempart_buf.h"
#endif /* PANDORA */


ProtocolVersion FrontendProtocol;

volatile sig_atomic_t InterruptPending = false;
volatile sig_atomic_t QueryCancelPending = false;
volatile sig_atomic_t ProcDiePending = false;
volatile sig_atomic_t CheckClientConnectionPending = false;
volatile sig_atomic_t ClientConnectionLost = false;
volatile sig_atomic_t IdleInTransactionSessionTimeoutPending = false;
volatile sig_atomic_t IdleSessionTimeoutPending = false;
volatile sig_atomic_t ProcSignalBarrierPending = false;
volatile sig_atomic_t LogMemoryContextPending = false;
volatile sig_atomic_t IdleStatsUpdateTimeoutPending = false;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;

int			MyProcPid;
pg_time_t	MyStartTime;
TimestampTz MyStartTimestamp;
struct Port *MyProcPort;
int32		MyCancelKey;
int			MyPMChildSlot;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
struct Latch *MyLatch;

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
char	   *DataDir = NULL;

/*
 * Mode of the data directory.  The default is 0700 but it may be changed in
 * checkDataDir() to 0750 if the data directory actually has that mode.
 */
int			data_directory_mode = PG_DIR_MODE_OWNER;

char		OutputFileName[MAXPGPATH];	/* debugging output file */

char		my_exec_path[MAXPGPATH];	/* full path to my executable */
char		pkglib_path[MAXPGPATH]; /* full path to lib directory */

#ifdef EXEC_BACKEND
char		postgres_exec_path[MAXPGPATH];	/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

BackendId	MyBackendId = InvalidBackendId;

BackendId	ParallelLeaderBackendId = InvalidBackendId;

Oid			MyDatabaseId = InvalidOid;

Oid			MyDatabaseTableSpace = InvalidOid;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
char	   *DatabasePath = NULL;

pid_t		PostmasterPid = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
bool		IsPostmasterEnvironment = false;
bool		IsUnderPostmaster = false;
bool		IsBinaryUpgrade = false;
bool		IsBackgroundWorker = false;

bool		ExitOnAnyError = false;

int			DateStyle = USE_ISO_DATES;
int			DateOrder = DATEORDER_MDY;
int			IntervalStyle = INTSTYLE_POSTGRES;

bool		enableFsync = true;
bool		allowSystemTableMods = false;
int			work_mem = 4096;
double		hash_mem_multiplier = 2.0;
int			maintenance_work_mem = 65536;
int			max_parallel_maintenance_workers = 2;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
#ifdef PANDORA
int			NBuffers = 1024 * 128;
#else /* !PANDORA */
int			NBuffers = 1000;
#endif /* PANDORA */

#ifdef DIVA /* Global variables */

#ifdef PLEAF_NUM_PAGE
int		NPLeafBuffers = PLEAF_NUM_PAGE;
#else
/* Default : 256 MiB (page: 4 KiB) */
int		NPLeafBuffers = 1024 * 64;
// int		NPLeafBuffers = 100 * 10;
#endif /* PLEAF_NUM_PAGE */

#ifdef PLEAF_NUM_INSTANCE
int		NPLeafInstances = PLEAF_NUM_INSTANCE; /* We use 4-bit */
#else
int		NPLeafInstances = 4;
#endif /* PLEAF_NUM_INSTANCE */

#ifdef PLEAF_INIT_PAGES
/* NPLeafInitPages is the number of pages per instance in initialization */
int NPLeafInitPages = PLEAF_INIT_PAGES;
#else
int NPLeafInitPages = 0;
#endif /* PLEAF_INIT_PAGES */

#ifdef EBI_NUM_PAGE
int		NEbiTreeBuffers = EBI_NUM_PAGE;
int   NEbiSubBuffers = EBI_NUM_PAGE;
#else
/* Default : 8 GiB (page: 8 KiB) */
int		NEbiTreeBuffers = 1024 * 1024;
// int		NEbiTreeBuffers = 10 * 10;
int   NEbiSubBuffers = 1024 * 1024;
// int   NEbiSubBuffers = 10 * 10;
#endif  /* EBI_NUM_PAGE */

#ifdef EBI_RING_NUM_PAGE
/* TODO: Default : 40M */
int   NReadBuffers = EBI_RING_NUM_PAGE;
#else
int   NReadBuffers = 10 * 50;
#endif  /* EBI_SUB_PAGE */

#endif /* DIVA */

#ifdef PANDORA
int NPandoraMempartBuffers = PANDORA_MEMPART_BUFFER_TOTAL_PAGE_COUNT;
int NPandoraMempartCount = PANDORA_MEMPART_COUNT;
int PandoraMempartPageCount = PANDORA_MEMPART_PAGE_COUNT;
int PandoraMempartStart = -1;
#endif /* PANDORA */

int			MaxConnections = 90;
int			max_worker_processes = 8;
int			max_parallel_workers = 8;
int			MaxBackends = 0;

int			VacuumCostPageHit = 1;	/* GUC parameters for vacuum */
int			VacuumCostPageMiss = 2;
int			VacuumCostPageDirty = 20;
int			VacuumCostLimit = 200;
double		VacuumCostDelay = 0;

int64		VacuumPageHit = 0;
int64		VacuumPageMiss = 0;
int64		VacuumPageDirty = 0;

int			VacuumCostBalance = 0;	/* working state for vacuum */
bool		VacuumCostActive = false;
