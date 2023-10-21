/*-------------------------------------------------------------------------
 *
 * pandora_relation_meta_file.c
 *
 * Pandora Relation Meta Implementation
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
 *    src/backend/pandora/partitioning/pandora_relation_meta_file.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef PANDORA
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dynahash.h"
#include "utils/resowner_private.h"

#include "pandora/pandora_relation_meta.h"

static void
PandoraGetRelationMetaFilename(const Oid relationOid, char *filename)
{
	snprintf(filename, 32, "relation_meta/%d", relationOid);
}

/*
 * PandoraCreateRelationMetaFile
 *
 * Make a new file for corresponding relation oid.
 */
void
PandoraCreateRelationMetaFile(const Oid relationOid)
{
	int fd;
	char filename[32];

	PandoraGetRelationMetaFilename(relationOid, filename);
	fd = open(filename, O_RDWR | O_CREAT | O_SYNC, (mode_t)0600);

	if (fd < 0)
	{
		fprintf(stderr, 
			"pandora relation meta file create error: %s", strerror(errno));
		Assert(false);
	}

	close(fd);
}

/*
 * PandoraOpenRelationMetaFile
 *
 * Open relation meta file.
 * Caller have to call 'PandoraCloseRelationMetaFile' after file I/O is done.
 */
static int
PandoraOpenRelationMetaFile(Oid relationOid)
{
	int fd;
	char filename[32];

	PandoraGetRelationMetaFilename(relationOid, filename);

	fd = open(filename, O_RDWR | O_SYNC, (mode_t) 0600);
	Assert(fd >= 0);

	return fd;
}

/*
 * EbiSubCloseSegmentFile
 *
 * Close relation meta file.
 */
static void
PandoraCloseRelationMetaFile(int fd)
{
	Assert(fd >= 0);

	if (close(fd) == -1)
	{
		fprintf(stderr, "relation meta file close error, %s\n", strerror(errno));
		Assert(false);
	}
}


/*
 * PandoraReadRelationMetaFile
 */
bool
PandoraRelationMetaFileExists(const Oid relationOid)
{
	char filename[32];
	
	PandoraGetRelationMetaFilename(relationOid, filename);

	if (access(filename, F_OK) != -1)
		return true;
	else
		return false;
}


/*
 * PandoraReadRelationMetaFile
 */
void
PandoraReadRelationMetaFile(const Oid relationOid, char *block)
{
	ssize_t read;
	int fd;

	fd = PandoraOpenRelationMetaFile(relationOid);

read_retry:
	read = pg_pread(fd, block, RELATION_META_BLOCKSIZE, 0);
	if (read < 0 && errno == EINTR)
		goto read_retry;

	Assert(read == RELATION_META_BLOCKSIZE);

	PandoraCloseRelationMetaFile(fd);
}

/*
 * PandoraWriteRelationMetaFile
 */
void
PandoraWriteRelationMetaFile(const Oid relationOid, char *block)
{
	int fd;
	ssize_t written;

	fd = PandoraOpenRelationMetaFile(relationOid);

write_retry:
	written = pg_pwrite(fd, block, RELATION_META_BLOCKSIZE, 0);
	if (written < 0 && errno == EINTR)
		goto write_retry;

	Assert(written == RELATION_META_BLOCKSIZE);

	PandoraCloseRelationMetaFile(fd);
}

#endif /* PANDORA */
