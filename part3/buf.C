/**
 * @file buf.C
 * Author: Shrey Katyal
 * ID: 9086052256
 * Author: Hassan Murayr
 * ID:
 * Author: Michael Tran
 * ID: 9083087123
 * Description: Implementation of the BufMgr class, and Minirel Buffer Management system
 */
#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/**
 * Allocates a buffer frame using the clock replacement policy.
 *
 * @param frame -The frame number allocated
 * @return -  Status OK if successful or BUFFEREXCEEDED if all buffer frames are pinned
 */
const Status BufMgr::allocBuf(int &frame)
{
    int checked = 0;
    while (checked++ < numBufs * 2)
    {
        advanceClock();
        BufDesc *curBuf = &bufTable[clockHand];
        
        if (!curBuf->valid) {
            frame = clockHand;
            curBuf->Clear();
            return OK;
        }
        
        if (curBuf->refbit) {
            curBuf->refbit = false;
            continue;
        }
        
        if (curBuf->pinCnt > 0)
            continue;
            
        if (curBuf->dirty && curBuf->file->writePage(curBuf->pageNo, &bufPool[clockHand]) != OK)
            return UNIXERR;
            
        if (curBuf->valid && hashTable->remove(curBuf->file, curBuf->pageNo) != OK)
            return HASHTBLERROR;
            
        frame = clockHand;
        curBuf->Clear();
        return OK;
    }
    return BUFFEREXCEEDED;
}

/**
 * Reads a page from the given file and puts it in the buffer pool.
 * If the page is already in the buffer pool, it increments the pin count
 * and sets the reference bit.  If the page is not in the buffer pool,
 * it allocates a frame, reads the page into the frame, inserts the page
 * in the buffer pool, and sets the pin count and reference bit.
 *
 * @param file The file from which to read the page.
 * @param PageNo The number of the page to read.
 * @param page A pointer to the page in the buffer pool.
 * @return Returns OK if no errors occurred, UNIXERR if a Unix error occurred,
 *  BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash
 *  table error occurred.
 */
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    
    int framePointer = 0;
    Status pageStatus = hashTable->lookup(file, PageNo, framePointer);

    if (pageStatus == OK)
    {
        BufDesc &currentFrame = bufTable[framePointer];
        currentFrame.pinCnt++;
        currentFrame.refbit = true;
        page = &(bufPool[framePointer]);
        return OK;
    }
    else if(pageStatus == HASHNOTFOUND)
    {
        Status allocStatus = allocBuf(framePointer);
        if(allocStatus != OK)
        {
            return allocStatus;
        }
        Status readStatus = file->readPage(PageNo, &(bufPool[framePointer]));
        if(readStatus != OK)
        {
            return readStatus;
        }
        Status insertStatus = hashTable->insert(file, PageNo, framePointer);
        if(insertStatus != OK){
            return insertStatus;
        }
        BufDesc &currentFrame = bufTable[framePointer];
        currentFrame.Set(file, PageNo);
        page = &(bufPool[framePointer]);
        return OK;
    }
    return pageStatus;
}

/**
 * Decrements the pin count of a page in the buffer pool.
 *
 * @param  - file    The file containing the page
 * @param  - PageNo  The page number within the file
 * @param  - dirty   If true, the page is marked as dirty, indicating it has been modified
 * @return  - Status OK if successful, HASHNOTFOUND if the page isn't in the buffer pool,
 *         PAGENOTPINNED if the page is found but has a pin count of 0
 */const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int framePointer = 0;
    Status pageStatus = hashTable->lookup(file, PageNo, framePointer);

    if (pageStatus != OK)
    {
        return HASHNOTFOUND;
    }

    BufDesc &currentFrame = bufTable[framePointer];
    if (currentFrame.pinCnt > 0)
    {
        currentFrame.pinCnt--;
        currentFrame.dirty = currentFrame.dirty | dirty;
        return OK;
    }

    return PAGENOTPINNED;
}

// Hassan
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
