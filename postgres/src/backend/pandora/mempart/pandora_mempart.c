/*-------------------------------------------------------------------------
 *
 * pandora_mempart.c
 *
 * Pandora Mempart Implementation
 *
 * IDENTIFICATION
 *    src/backend/pandora/mempart/pandora_mempart.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pandora/pandora_mempart_buf.h"

#ifdef PANDORA
/*
 * PandoraGetMempartIdFromBufId
 *		Get mempartition id from the given buffer id.
 *
 * PANDORA's mempartition buffer is located in the end of traditional buffer
 * pool and it start from PandoraMempartStart buffer id.
 *
 * All mempartitions have same size, PandoraMempartPageCount. Using these
 * informations, change the given buffer id to the mempartition id.
 *
 * Note that if the given buf_id is not in the mempartition buffers, return -1.
 */
inline int
PandoraGetMempartIdFromBufId(int buf_id)
{
	if (!PandoraCheckMempartBuf(buf_id))
		return -1;

	return (buf_id - PandoraMempartStart) / PandoraMempartPageCount;
}

/*
 * Return the mempartition's LWLock pointer.
 */
inline LWLock *
PandoraGetMempartLock(int mempart_id)
{
	LWLockPadded *lock = PandoraMempartLocks + mempart_id;
	return &lock->lock;
}

/*
 * PandoraGetMempartBufId
 *		Get mempartition's buffer id.
 *
 * PANDORA's mempartitions are located in the traditional buffer pool. So
 * mempartitions has buffer id too.
 *
 * Using the given buf_id, calculate mempartition's start buffer id. When the
 * given buf_id is not located in mempartition buffers, return -1.
 */
inline int
PandoraGetMempartBufId(int buf_id)
{
	if (!PandoraCheckMempartBuf(buf_id))
		return -1;

	return PandoraMempartStart +
		(PandoraGetMempartIdFromBufId(buf_id) * PandoraMempartPageCount);
}

/*
 * Return true when the given buffer is located in mempart buffer.
 */
inline bool
PandoraCheckMempartBuf(int buf_id)
{
	return buf_id >= PandoraMempartStart;
}

BlockNumber
PandoraGetMempartBlockCount(RelFileNode node, PandoraPartNumber partNum)
{
	return PandoraNblocks(node, 0, partNum, 0);
}

#else /* !PANDORA */

inline int
PandoraGetMempartIdFromBufId(int buf_id)
{
	return -1;
}

inline LWLock *
PandoraGetMempartLock(int mempart_id)
{
	return NULL;
}

inline int
PandoraGetMempartBufId(int buf_id)
{
	return -1;
}

/*
 * Return true when the given buffer is located in mempart buffer.
 */
inline bool
PandoraCheckMempartBuf(int buf_id)
{
	return false;
}
#endif /* PANDORA */
