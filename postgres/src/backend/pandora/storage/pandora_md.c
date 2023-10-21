/*-------------------------------------------------------------------------
 *
 * pandora_md.c
 *
 * Pandora Magnetic Disk Implementation
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
 *    src/backend/pandora/partitioning/pandora_md.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef PANDORA
#include "postgres.h"

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "catalog/pg_tablespace_d.h"
#include "utils/wait_event.h"
#include "port/pg_iovec.h"

#include "pandora/pandora_md.h"
#include "pandora/pandora_mempart_buf.h"

#ifdef STAT_IO_URING
extern bool isOLAP;
extern uint64 io_uring_submitTime;
#endif /* STAT_IO_URING */

/*
 * PandoraNblocks
 *		Get the number of blocks of the file.
 *
 * Called when the partitioning worker does partitioning.
 */
BlockNumber
PandoraNblocks(RelFileNode node, PandoraPartLevel partLvl,
			   PandoraPartNumber partNum, PandoraPartGenNo partGen)
{
	char		   *path;
	int				fd;
	BlockNumber		blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was not created yet */
			pfree(path);
			return 0;
		}
		else
		{
			Abort("Failed to open pandora partition file");
			ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
								  path));
		}
	}

	/* Get file size and compute new blockNum */
	blockNum = PandoraLseekBlocks(fd);

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	if (unlikely(blockNum == 0))
	{
#ifdef PANDORA_DEBUG
		fprintf(stderr, "[PANDORA] PandoraNblocks, path: %s, nblocks: %u -> %u\n",
						path, blockNum, PandoraMempartPageCount);
#endif /* PANDORA DEBUG */
		/*
		 * If blockNum is 0, it means that this file is a level 0 mempartition
		 * and has not yet been flushed after being filled in-memory.
		 * Therefore, let blockNum be the maximum number of blocks in the
		 * mempartition. 
		 */
		Assert(partLvl == 0);
		blockNum = PandoraMempartPageCount;
	}
#ifdef PANDORA_DEBUG
	else
	{
		fprintf(stderr, "[PANDORA] PandoraNblocks, path: %s, nblocks: %u\n",
						path, blockNum);
	}
#endif /* PANDORA DEBUG */

	pfree(path);
	return blockNum;
}

/*
 * PandoraCreatePartitionFile
 */
void
PandoraCreatePartitionFile(RelFileNode node, PandoraPartLevel partLvl,
			  PandoraPartNumber partNum, PandoraPartGenNo partGen)
{
	char		   *path;
	int				fd;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Check existing partition file. */
	if (access(path, F_OK) == 0)
	{
		/* The file is already exists. */
		elog(WARNING, "pandora partition file is already created, name: %s", 
								path);

		pfree(path);
		return;
	}

	/* Open(or create) the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR | O_CREAT | O_SYNC)) == -1)
	{
		Abort("Failed to open pandora partition file");
		ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
							  path));
	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));

	/* Resource cleanup. */
	pfree(path);
}


/*
 * PandoraExtend
 *		Extend the partition file.
 *
 * Called when the partitioning worker does partitioning.
 */
void
PandoraExtend(RelFileNode node, PandoraPartLevel partLvl,
			  PandoraPartNumber partNum, PandoraPartGenNo partGen,
			  BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd;
	off_t			seekpos;
	int				nbytes;

	/* Get position to write zero page */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open(or create) the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR | O_CREAT | O_SYNC)) == -1)
	{
		Abort("Failed to open pandora partition file");
		ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
							  path));
	}

retry:
	/* Write and sync zero page */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
	nbytes = pg_pwrite(fd, buffer, BLCKSZ, seekpos);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nbytes != BLCKSZ && errno == 0)
		errno = ENOSPC;

	if (nbytes < -1)
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend pandora partition file \"%s\": %m",
							path),
					 errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend pandora partition file \"%s\": wrote only %d of %d bytes at block %u",
						path, nbytes, BLCKSZ, blockNum),
				 errhint("Check free disk space.")));
	}

	pfree(path);
}

/*
 * PandoraMempartExtend
 *		Extend the partition file.
 *
 * Called when the partitioning worker does partitioning.
 */
void
PandoraMempartExtend(RelFileNode node, PandoraPartLevel partLvl,
			  PandoraPartNumber partNum, PandoraPartGenNo partGen,
			  BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd;
	// int				nbytes;
	// off_t			seekpos = 0;
	// int				wsize = BLCKSZ; // BLCKSZ * PANDORA_MEMPART_PAGE_COUNT

	Assert(blockNum == 0);

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open(or create) the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR | O_CREAT | O_SYNC)) == -1)
	{
		Abort("Failed to open pandora partition file");
		ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
							  path));
	}

	/* Mempart does not have to change EOF early */
// retry:
// 	/* Write and sync zero page */
// 	errno = 0;
// 	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
// 	nbytes = pg_pwrite(fd, buffer, wsize, seekpos);
// 	pgstat_report_wait_end();

// 	/* if write didn't set errno, assume problem is no disk space */
// 	if (nbytes != wsize && errno == 0)
// 		errno = ENOSPC;

// 	if (nbytes < -1)
// 	{
// 		/*
// 		 * See comments in FileRead()
// 		 */
// #ifdef WIN32
// 		DWORD		error = GetLastError();

// 		switch (error)
// 		{
// 			case ERROR_NO_SYSTEM_RESOURCES:
// 				pg_usleep(1000L);
// 				errno = EINTR;
// 				break;
// 			default:
// 				_dosmaperr(error);
// 				break;
// 		}
// #endif
// 		/* OK to retry if interrupted */
// 		if (errno == EINTR)
// 			goto retry;
// 	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	// if (nbytes != wsize)
	// {
	// 	if (nbytes < 0)
	// 		ereport(ERROR,
	// 				(errcode_for_file_access(),
	// 				 errmsg("could not extend file \"%s\": %m",
	// 						path),
	// 				 errhint("Check free disk space.")));
	// 	/* short write: complain appropriately */
	// 	ereport(ERROR,
	// 			(errcode(ERRCODE_DISK_FULL),
	// 			 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
	// 					path, nbytes, wsize, blockNum),
	// 			 errhint("Check free disk space.")));
	// }

	pfree(path);
}


/*
 * PandoraWrite
 *		Write single partition page.
 *
 * Called when evicting a partition data block from PostgreSQL's traditional
 * buffer pool.
 */
bool
PandoraWrite(RelFileNode node, PandoraPartLevel partLvl,
			 PandoraPartNumber partNum, PandoraPartGenNo partGen,
			 BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd;
	off_t			seekpos;
	int				nbytes;

	/* A relation using pandora must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);

	/* Get position to write */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was GC'd, so just return */
			pfree(path);
			return false;
		}
		else
		{
			Abort("Failed to open pandora partition file");
			ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
								  path));
		}
	}

retry:
	/* Write data */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	nbytes = pg_pwrite(fd, buffer, BLCKSZ, seekpos);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nbytes != BLCKSZ && errno == 0)
		errno = ENOSPC;

	if (nbytes < -1)
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in pandora partition file \"%s\": %m",
							blockNum, path)));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in pandora partition file \"%s\": wrote only %d of %d bytes",
						blockNum,
						path,
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	pfree(path);
	return true;
}

/*
 * PandoraWriteBatch
 *		Write multiple partition page.
 */
bool
PandoraWriteBatch(RelFileNode node, PandoraPartLevel partLvl,
			 PandoraPartNumber partNum, PandoraPartGenNo partGen,
			 uint64_t offset, struct iovec *iovs, int64_t iovcnt)
{
	int64_t		totalNbytes = 0;
	struct iovec *curIovs = iovs;
	int64_t 	iovsum = 0;
	int64_t		curIovcnt;
	int64_t	curOffset;
	char	*path;
	int		fd;
	int64_t		nbytes;

	/* A relation using pandora must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR | O_SYNC)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was GC'd, so just return */
			pfree(path);
			return false;
		}
		else
		{
			Abort("Failed to open pandora partition file");
			ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
								  path));
		}
	}

	for (;;)
	{
		/* Write is done. */
		if (iovcnt == iovsum)
			break;

		/* 
		 * Set iovcnt and iovector address, considering limited i/o vector's 
		 * maximum.
		 */
		curIovs = (struct iovec *) ((char *) iovs + sizeof(struct iovec) * iovsum);
		curOffset = offset + iovsum * BLCKSZ;
		curIovcnt = iovcnt - iovsum;
		if (curIovcnt > IOV_MAX)
			curIovcnt = IOV_MAX;

retry:
		/* Write data */
		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
		nbytes = pg_pwritev(fd, curIovs, curIovcnt, curOffset);
		pgstat_report_wait_end();

		/* if write didn't set errno, assume problem is no disk space */
		if (nbytes != BLCKSZ * curIovcnt && errno == 0)
		{
			errno = ENOSPC;
			elog(WARNING, "no disk space to write pages");
			break;
		}

		if (nbytes < -1)
		{
			/*
			 * See comments in FileRead()
			 */
#ifdef WIN32
			DWORD		error = GetLastError();

			switch (error)
			{
				case ERROR_NO_SYSTEM_RESOURCES:
					pg_usleep(1000L);
					errno = EINTR;
					break;
				default:
					_dosmaperr(error);
					break;
			}
#endif
			/* OK to retry if interrupted */
			if (errno == EINTR)
				goto retry;
		}

		iovsum += curIovcnt;
		totalNbytes += nbytes;
	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	if (totalNbytes != BLCKSZ * iovcnt)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write offset %lu in pandora partition file \"%s\": %m",
							offset, path)));

		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write offset %lu in pandora partition file \"%s\": wrote only %ld of %d bytes",
						offset,
						path,
						totalNbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}
		
	/* Resource cleanup. */
	pfree(path);

	return true;
}

/*
 * PandoraWriteback
 *		Write single partition page back to storage. (sync)
 *
 * Flushes partition pages to storage from OS cache.
 */
void
PandoraWriteback(RelFileNode node, PandoraPartLevel partLvl,
				 PandoraPartNumber partNum, PandoraPartGenNo partGen,
				 BlockNumber blockNum, BlockNumber nblocks)
{
	char		   *path;
	int				fd;
	off_t			seekpos;

	/* Get position to flush */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was GC'd, so just return */
			pfree(path);
			return;
		}
		else
		{
			Abort("Failed to open pandora partition file");
			ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
								  path));
		}
	}

	/* Flush data */
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_FLUSH);
	pg_flush_data(fd, seekpos, (off_t) BLCKSZ * nblocks);
	pgstat_report_wait_end();

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	pfree(path);
}

/*
 * PandoraRead
 *		Read single partition page.
 *
 * Called when reading a partition data block to PostgreSQL's traditional buffer
 * pool.
 */
void
PandoraRead(RelFileNode node, PandoraPartLevel partLvl,
			PandoraPartNumber partNum, PandoraPartGenNo partGen,
			BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd;
	off_t			seekpos;
	int				nbytes;

	/* A relation using pandora must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);

	/* Get position to read */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR)) == -1)
	{
		Abort("Failed to open pandora partition file");
		ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
							  path));
	}

retry:
	/* Read data */
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
	nbytes = pg_pread(fd, buffer, BLCKSZ, seekpos);
	pgstat_report_wait_end();

	if (nbytes < 0)
	{
		/*
		 * Windows may run out of kernel buffers and return "Insufficient
		 * system resources" error.  Wait a bit and retry to solve it.
		 *
		 * It is rumored that EINTR is also possible on some Unix filesystems,
		 * in which case immediate retry is indicated.
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	if (nbytes != BLCKSZ)
	{
		Abort("Could not read block in pandora partition file");
		if (nbytes < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in pandora partition file \"%s\": %m",
							blockNum, path)));
		}

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read block %u in pandora partition file \"%s\": read only %d of %d bytes",
							blockNum, path,
							nbytes, BLCKSZ)));
	}

	pfree(path);
}

/*
 * PandoraRequest
 *		Request multi pages using io_uring.
 *
 * The file must have been opened by the caller.
 */
int
PandoraRequest(struct io_uring* ring, int fd, struct iovec *iovs, 
			   BlockNumber blockNum, int req_page_num, void *io_info)
{
#ifdef STAT_IO_URING
	uint64 io_uring_diff;
	struct timespec io_uring_beginStamp;
	struct timespec io_uring_endStamp;
#endif /* STAT_IO_URING */

	int						returnCode;
	off_t					seekpos;
	struct io_uring_sqe	   *sqe;

	seekpos = (off_t) BLCKSZ * blockNum;

	sqe = io_uring_get_sqe(ring);

	/* set read request */
	io_uring_prep_readv(sqe, fd, iovs, req_page_num, seekpos);

	/* set user data */
	io_uring_sqe_set_data(sqe, io_info);

	/* submit request */
#ifdef STAT_IO_URING
	if (isOLAP)
		clock_gettime(CLOCK_MONOTONIC, &io_uring_beginStamp);

	returnCode = io_uring_submit(ring);

	if (isOLAP)
	{
		clock_gettime(CLOCK_MONOTONIC, &io_uring_endStamp);

		io_uring_diff =
			(io_uring_endStamp.tv_sec * 1000000000 + io_uring_endStamp.tv_nsec) -
		  (io_uring_beginStamp.tv_sec * 1000000000 + io_uring_beginStamp.tv_nsec);
		io_uring_submitTime += io_uring_diff;
	}
#else /* !STAT_IO_URING*/
	returnCode = io_uring_submit(ring);
#endif /* STAT_IO_URING */

	if (returnCode < 0)
		elog(ERROR, "FileRequest error");

	return returnCode;
}

bool
PandoraMempartWrite(RelFileNode node, PandoraPartLevel partLvl,
			 PandoraPartNumber partNum, PandoraPartGenNo partGen,
			 char *buffer, int nblock)
{
	char		   *path;
	int				fd;
	off_t			seekpos;
	int				nbytes;

	/* A relation using pandora must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);
	Assert(nblock < (INT_MAX / BLCKSZ));

	/* Get position to write */
	seekpos = 0;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR | O_SYNC)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was GC'd, so just return */
			pfree(path);
			return false;
		}

		Abort("Failed to open pandora partition file");
		ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
							  path));
	}

retry:
	/* Write data */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	nbytes = pg_pwrite(fd, buffer, BLCKSZ * nblock, seekpos);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nbytes != BLCKSZ * nblock && errno == 0)
		errno = ENOSPC;

	if (nbytes < -1)
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	if (nbytes != BLCKSZ * nblock)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write in pandora partition file \"%s\": %m",
							path)));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write in pandora partition file \"%s\": wrote only %d of %d bytes",
						path,
						nbytes, BLCKSZ * nblock),
				 errhint("Check free disk space.")));
	}

	pfree(path);
	return true;
}

/*
 * PandoraMempartRead
 *		Read mempartition page into mempartition buffer.
 *
 * Called when the target mempartion doesn't exist in mempartition buffer pool.
 * 
 * In almost all cases, all transactions except INSERT that try to read a page
 * from a mempartition will read the page by calling PandoraRead(), not
 * PandoraMempartRead().
 * The only case where PandoraMempartRead() will be called is if the INSERT
 * transaction tries to insert a new record into the most recent mempartition
 * and it is not in the buffer, but in storage, and this will only happen once,
 * the first time PostgreSQL starts.
 */
void
PandoraMempartRead(RelFileNode node, PandoraPartLevel partLvl,
				   PandoraPartNumber partNum, PandoraPartGenNo partGen,
				   char *buffer)
{
	char		   *path;
	int				fd;
	int				nblock;
	int				nbytes;

	elog(LOG, "PandoraMempartRead");
	// sleep(15);

	/* A relation using pandora must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = PandoraSimpleOpen(path, O_RDWR)) == -1)
	{
		Abort("Failed to open pandora partition file");
		ereport(ERROR, errmsg("Failed to open pandora partition file, name: %s",
							  path));
	}

retry:
	nblock = PandoraLseekBlocks(fd);
	Assert(nblock < (INT_MAX / BLCKSZ));
	Assert(nblock != 0);

	/* Read data */
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
	nbytes = pg_pread(fd, buffer, nblock * BLCKSZ, 0);
	pgstat_report_wait_end();

	if (nbytes < 0)
	{
		/*
		 * Windows may run out of kernel buffers and return "Insufficient
		 * system resources" error.  Wait a bit and retry to solve it.
		 *
		 * It is rumored that EINTR is also possible on some Unix filesystems,
		 * in which case immediate retry is indicated.
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (PandoraSimpleClose(fd) == -1)
	{
		ereport(ERROR, errmsg("Failed to close pandora partition file, name: %s",
							  path));
	}

	if (nbytes != nblock * BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read pandora mempartition file \"%s\": %m",
							path)));

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read pandora mempartition file \"%s\": read only %d of %d bytes",
							path,
							nbytes, nblock * BLCKSZ)));
	}

	pfree(path);
}
#endif /* PANDORA */
