/* =======================================================================
 *   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of version 2 the GNU General Public License as
 *   published by the Free Software Foundation.
 *   
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *   
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * =======================================================================
 */
/*
 * @(#)$RCSfile: bs_index.h,v $ $Revision: 1.1.36.3 $ (DEC) $Date: 2006/01/24 07:27:40 $
 */

#ifndef _INDEX_H_
#define _INDEX_H_

#include <msfs/bs_ods.h>

/*
 * Indicate to the idx_close_index_file routine the proper
 * actions to take.
 */

typedef enum
{
    IDX_NORMAL_CLOSE,
    IDX_REMOVING_DIR,
    IDX_RECOVERY
} idxCloseActionT;

#ifdef KERNEL

statusT
idx_remove_index_file(
            bfAccessT *dir_bfap, /* Directory access structure */
            ftxHT ftxH           /* Parent transaction */
            );

statusT
idx_open_index_file(
    bfAccessT*  dir_bfap,     /* directory's access structure. */
    ftxHT ftxH                /* Parent transaction */
    );

void
idx_close_index_file(
    bfAccessT*  dir_bfap,     /* index file's access structure */
    idxCloseActionT action    /* action to perform */
    );

statusT
idx_insert_filename(
    bfAccessT *dir_bfap,     /* Directory's access structure */
    struct nameidata *ndp,   /* Namei struct of file to insert */
    int page,                /* Page to insert onto */
    long count,              /* Page offset to insert to */
    ftxHT  parentFtx         /* Parent Ftx */
    );

statusT
idx_remove_filename(
    bfAccessT *dir_bfap,       /* Directory's access structure */
    struct nameidata *ndp,     /* Namei struct of file to remove */
    int page,                  /* Page to insert onto */
    long count,                /* Page offset to insert to */
    ftxHT  parentFtx           /* Parent ftx */
    );

statusT
idx_lookup_filename(
    bfAccessT   *dir_bfap,       /* bfap of the directory */
    struct nameidata *ndp,       /* Namei struct of file to lookup */
    bfTagT      *found_bs_tag,   /* if found files tag # */
    fs_dir_entry **found_buffer, /* pointer to directory record */
    rbfPgRefHT  *pgref,          /* pgref for pinning record */
    int         flag,	         /* flag: PIN_PAGE return pointer to
                                 /* dir entry with page pinned */
    ftxHT  ftxH                  /* parent ftx */ 
    );

statusT
idx_directory_insert_space(
    bfAccessT *dir_bfap, /* Directories Access Structure */
    ulong size,         /* size of space to insert */
    int insert_page,     /* Page where space lives */
    uint32T insert_count,    /* Offset where space lives */
    uint32T unglom_offset,  /* offset to be undone */
    uint32T unglom_size,    /* size to be undone */
    ftxHT  parentFtx,      /* Parent Ftx */
    int forceflag	/* sometimes we need to ignore quotas */
    );

int
idx_directory_get_space(
    bfAccessT *dir_bfap, /* The directory's access structure */
    ulong size,        /* Requested size to obtain */
    int *insert_page,    /* out - page where storage resides */
    long *insert_count,   /* out - offset where storage resides */
    ftxHT  parent_ftx   /* The parent transaction */
    );

long
idx_setup_for_truncation(
    bfAccessT *dir_bfap,
    int operation,
    ftxHT parentFtxH
    );

statusT
idx_convert_dir(
    bfAccessT *dir_bfap,
    struct nameidata *ndp,
    ftxHT ftxH
    );

statusT
idx_prune_start(
    bfAccessT *dir_bfap,
    ftxHT ftxH
    );
void     
idx_prune_finish(
    bfAccessT *dir_bfap
    );

void
idx_undo_opx(
    ftxHT ftxH,
    int opRecSz,
    void *opRec
    );

struct bsUnkIdxRec;
statusT
bf_idx_file_test(
    struct bsUnkIdxRec *idxrec,
    ulong flagval
    );

/* This structure will be cached and pointed to by the directory's
 * access structure.  */

/* The index_tag field of this record will be written to the 
 * directory's new MCELL record.
 */


typedef struct bsDirIdxRec
{
    ulong flags; /* This field must not move ! */
    bfAccessT * idx_bfap;
    bfTagT index_tag;
}bsDirIdxRecT;

#endif /* KERNEL */

/* This record will be a new MCELL record associated with the index *
 *  file's MCELL. It will be updated every time there is a change to
 *  the * index file (and correspondingly the directory).  It must be
 *  called * after fs_update_stats for the directory in order to
 *  insure an * up-to-date modification time is saved.
 * 
 * This packet will also be pointed to by the index file's access
 * structure.  */
  
typedef struct bsIdxBmtRec
{
    uint32T fname_page;
    uint32T ffree_page;
    uint32T fname_levels;
    uint32T ffree_levels;
}bsIdxBmtRecT;

#ifdef KERNEL

typedef struct bsIdxRec
{
    ulong flags;          /* This field must not move ! */
    bsIdxBmtRecT bmt;
    bfAccessT *dir_bfap;
    ulong prune_key_ffree;
    ulong prune_key_fname;
}bsIdxRecT;

#endif /* KERNEL */

typedef struct bsUnkIdxRec
{
    ulong flags;
}bsUnkIdxRecT;

/* Truncation operations */

#define TRUNC_QUERY 0
#define TRUNC_UPDATE 1

/* Possible flags values */

#define IDX_TRUNCATE 1
#define IDX_INDEX_FILE 2

/* This is the record passed to the root done routine for pruning.  */

#define IDX_GET_PAGE_NO( _addr ) \
   ( (_addr) >> (PAGE_SHIFT) )

#define IDX_GET_OFFSET( _addr ) \
   ( (_addr) & (PAGE_MASK) )

#define IDX_GET_ADDRESS( _pg, _off ) \
   ( (((ulong) _pg) << (PAGE_SHIFT)) + (_off) )

#define IDX_INDEXING_ENABLED(_bfa) \
   ( (_bfa->idx_params != (void *) -1 ) && \
     (_bfa->idx_params != NULL) )

#define IDX_FILE_IS_DIRECTORY(_bfa) \
   ( bf_idx_file_test((_bfa)->idx_params, 0) ) 

#define IDX_FILE_IS_INDEX(_bfa) \
   ( bf_idx_file_test((_bfa)->idx_params, IDX_INDEX_FILE) ) 

/* This is the data contained in a node. Each node will contain an
 * array of these entries. The entries represent either the location
 * of the next child node or the location of the actual data.  */

typedef struct idxNodeEntry
{
    ulong search_key;
    union {
        ulong dir_offset;
        ulong node_page;
        ulong free_size;
        ulong any_field;
    }loc;
}idxNodeEntryT;

typedef struct idxNode
{
    ulong total_elements;
    union
    {
        int32T leftmost;   /* boolean indicating if node is 
                               leftmost node */
        int32T page_left;  /* Page number of left sibling node. */
    } sib;
    int32T page_right;     
    ulong reserved[2];
    idxNodeEntryT data[1];  /* Keep compiler happy. The size will be computed
                               below as IDX_MAX_ELEMENTS and checked at 
                               run time */
}idxNodeT;

/* This is actually the bounds for the above array.*/

#define IDX_MAX_ELEMENTS ((ADVFS_PGSZ                                     \
                          - sizeof(idxNodeT) + sizeof(idxNodeEntryT))     \
                          / sizeof(idxNodeEntryT)) 

#define IDX_MAX_BTREE_LEVELS 4 /* this gives us 254**4 files ~4 billion */
#define IDX_COMPRESS_ME  IDX_MAX_ELEMENTS>>2

/* This array is used for splitting nodes. It will be filled up
 * with the location of pinned pages and then passed recursively back
 * up through the b-tree for consumption. The array will be filled in
 * at the leaf node. This is necessary in order to postpone obtaining
 * resources until the point of no failure.  */

typedef struct idxPinnedPgs
{
    uint32T pgno;
    rbfPgRefHT pgref;
    idxNodeT *pgptr;
}idxPinnedPgsT;

typedef struct idxClupMsg
{
    uint32T delCnt;
    void * delList;
}idxClupMsgT;

typedef struct idxPruneMsgs
{
    uint32T msg_cnt;
    idxClupMsgT msgs[IDX_MAX_BTREE_LEVELS];
}idxPruneMsgsT;

/* This record will be used by all undo routines. The basic
 * information to undo a transaction can be captured in this single
 * record.  */

typedef enum 
{
    IDX_INSERT_FILENAME_UNDO,
    IDX_REMOVE_FILENAME_UNDO,
    IDX_DIRECTORY_INSERT_SPACE_UNDO,
    IDX_DIRECTORY_GET_SPACE_UNDO,
    IDX_SETUP_FOR_TRUNCATION_INT_UNDO,
    IDX_CREATE_UNDO
} idxUndoActionT;

typedef struct idxUndoRecord
{
    idxNodeEntryT nodeEntry;
    bfSetIdT bfSetId;
    bfTagT dir_tag;
    idxUndoActionT action;
    
}idxUndoRecordT; 

/* This is the point at which we open the index file for the directory
 * being openned. If the open is successful then we will associate the
 * index file with this diectories access strucutre.  
 * 
 * We opened and associated the index file to the directory. Then we
 * must determine if the directory we just opened was a clone.
 *
 * If the clone index file openned successfully. We can infer from this
 * that the original must also have an index file and the open of the
 * clone index file caused the original index file to be opened. Now
 * we only need to associated the orignal directory with the original
 * index
 *
 * If the clone directory does not have an index file. The original
 * directory may have grown to the point where it now has one. We must
 * open it and associated it with the directory if it exists
 *
 * If this call fails we will just ignore it since nothing will have
 * been setup. 
 *
 * NOTE: As an optimization a -1 is put in the idx_params field when a
 * directory is openned and it does not have an index file. This will
 * stay a -1 until either an index file is created or the bfap is
 * recycled. This saves a read of the mcell every time the directory
 * is openned and the bfap is found.
 */

#define IDX_NEED_INDEX_OPEN(_bfap) ((_bfap->idx_params != (void *)-1) && \
      (((bsDirIdxRecT *) _bfap->idx_params)->idx_bfap == NULL))

#define IDX_OPEN_INDEX(_bfap,_sts) \
{ \
    if (_bfap->idx_params != (void *) -1) \
    { \
        if (_bfap->dmnP->dmnVersion >= FIRST_INDEXED_DIRECTORIES_VERSION) \
        { \
              _sts=idx_open_index_file(_bfap,FtxNilFtxH); \
        } \
        else \
        { \
              _bfap->idx_params = (void *) -1; \
              _sts=EOK; \
        } \
    } \
    else _sts=EOK; \
}

#define IDX_CLOSE_INDEX(_bfap) \
{ \
   idx_close_index_file(_bfap,IDX_NORMAL_CLOSE); \
}


#define IDX_UPDATE_TRUNC_DIRECTORY(_bfap,_pgs,_ftx) \
{ \
    if (((bsIdxRecT *)((bsDirIdxRecT *)(_bfap)->idx_params)->idx_bfap->idx_params)->flags&IDX_TRUNCATE) \
    { \
        _pgs=idx_setup_for_truncation((_bfap),TRUNC_UPDATE,_ftx); \
    } \
    else \
    { \
        _pgs=0; \
    } \
}

#define IDX_QUERY_TRUNC_DIRECTORY(_bfap,_pgs,_ftx) \
{ \
    if (((bsIdxRecT *)((bsDirIdxRecT *)(_bfap)->idx_params)->idx_bfap->idx_params)->flags&IDX_TRUNCATE) \
    { \
        _pgs=idx_setup_for_truncation((_bfap),TRUNC_QUERY,_ftx); \
    } \
    else \
    { \
        _pgs=0; \
    } \
}

#define IDX_INC_TOTAL(_pgref,_nptr) \
{ \
    RBF_PIN_FIELD(_pgref,_nptr->total_elements); \
    _nptr->total_elements++; \
    MS_SMP_ASSERT(_nptr->total_elements <= IDX_MAX_ELEMENTS); \
}

#define IDX_DEC_TOTAL(_pgref,_nptr) \
{ \
    RBF_PIN_FIELD(_pgref,_nptr->total_elements); \
    _nptr->total_elements--; \
    MS_SMP_ASSERT(_nptr->total_elements <= IDX_MAX_ELEMENTS); \
}

#define IDX_GET_BFAP(_dir_bfap, _idx_bfap) \
{ \
    if (IDX_INDEXING_ENABLED(_dir_bfap)) { \
        _idx_bfap = ((bsDirIdxRecT *)_dir_bfap->idx_params)->idx_bfap; \
    } \
}

#endif /* _INDEX_H_ */
