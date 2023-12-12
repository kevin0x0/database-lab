/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <functional>
#include <memory>
#include <iostream>
#include <sys/types.h>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "types.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
  : numBufs(bufs) {
  bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  // write back all modified page before we free the resouces.
  for (FrameId id = 0; id < numBufs; ++id) {
    BufDesc& entry = bufDescTable[id];
    if (!entry.valid)
      continue;
    if (entry.pinCnt != 0)
      throw PagePinnedException(entry.file->filename(), entry.pageNo, id);
    if (entry.dirty)
      entry.file->writePage(bufPool[id]);
  }

  // free resouces.
  delete hashTable;
  delete[] bufPool;
  delete[] bufDescTable;
}

void BufMgr::advanceClock()
{
  clockHand = clockHand == numBufs - 1 ? 0 : clockHand + 1;
}

void BufMgr::allocBuf(FrameId & frame) 
{
  int count = 0;
  int trylimit = numBufs * 2;
  while (count++ <= trylimit) {
    advanceClock();

    BufDesc& entry = bufDescTable[clockHand];
    if (!entry.valid) { // invalid(not alllocated) entry
      entry.Clear();
      frame = clockHand;
      return;
    }
    if (entry.refbit) {
      entry.refbit = false;
      continue;
    }
    if (entry.pinCnt != 0)
      continue;

    // now we get an available buffer.

    if (entry.dirty)  // has been modified, write it back.
      entry.file->writePage(bufPool[clockHand]);

    // update mapping
    hashTable->remove(entry.file, entry.pageNo);
    entry.Clear();
    frame = clockHand;
    return;
  }

  throw BufferExceededException();
}

  
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
  try {
    FrameId id = numBufs;
    hashTable->lookup(file, pageNo, id);
    // now we know there is already a buffer corresponding to the page to be read.
    bufDescTable[id].refbit = true;
    ++bufDescTable[id].pinCnt;
    page = &bufPool[id];
  } catch (HashNotFoundException& e) {
    // there is no appropriate buffer, we have to allocate a new one.
    FrameId id = numBufs;
    allocBuf(id);
    // associate the page with the new buffer.
    bufPool[id] = file->readPage(pageNo);
    // update mapping.
    hashTable->insert(file, pageNo, id);
    bufDescTable[id].Set(file, pageNo);
    page = &bufPool[id];
  }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  try {
    FrameId id = numBufs;
    hashTable->lookup(file, pageNo, id);
    if (bufDescTable[id].pinCnt == 0)
      throw PageNotPinnedException(file->filename(), pageNo, id);
    --bufDescTable[id].pinCnt;
    if (dirty)
      bufDescTable[id].dirty = true;
  } catch (HashNotFoundException& e) {
    // no this page, do nothing
  }
}

void BufMgr::flushFile(const File* file) 
{
  for (FrameId id = 0; id < numBufs; ++id) {
    BufDesc& entry = bufDescTable[id];
    if (entry.file != file)
      continue;
    if (!entry.valid) // has right data, but invalid
      throw BadBufferException(id, entry.dirty, entry.valid, entry.refbit);
    if (entry.pinCnt != 0)  // there is already no reference to this page
      throw PagePinnedException(file->filename(), entry.pageNo, id);
    if (entry.dirty)  // write it back if dirty(modified)
      entry.file->writePage(bufPool[id]);
    // free buffer
    hashTable->remove(file, entry.pageNo);
    entry.Clear();
  }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page&& new_page = file->allocatePage();
  FrameId fid = numBufs;
  allocBuf(fid);
  // associate new page with new buffer.
  hashTable->insert(file, new_page.page_number(), fid);
  bufDescTable[fid].Set(file, new_page.page_number());
  bufPool[fid] = new_page;
  page = &bufPool[fid];
  pageNo = new_page.page_number();
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
  file->deletePage(PageNo);
  try {
    // see if there is any buffer corresponding to the page.
    FrameId fid = numBufs;
    hashTable->lookup(file, PageNo, fid);

    // now we know there is, free the buffer.
    hashTable->remove(file, PageNo);
    bufDescTable[fid].Clear();
  } catch (HashNotFoundException& e) {
    // no that buffer, do nothing
  }
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
  int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
    tmpbuf = &(bufDescTable[i]);
    std::cout << "FrameNo:" << i << " ";
    tmpbuf->Print();

    if (tmpbuf->valid == true)
      validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

} // namespace badgerdb closed
