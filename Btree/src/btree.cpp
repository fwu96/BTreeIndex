/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"


//#define DEBUG

typedef struct tuple {
    int i;
    double d;
    char s[64];
} RECORD;

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

    BTreeIndex::BTreeIndex(const std::string & relationName,
                           std::string & outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType)
    {
        // Generating an index file name
        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        std::string indexName = idxStr.str();
        // Initializing
        outIndexName = indexName;
        attributeType = attrType;
        scanExecuting = false;
        bufMgr = bufMgrIn;
        this -> attrByteOffset = attrByteOffset;
        headerPageNum = 1;
        leafOccupancy = 0;
        nodeOccupancy = 0;
        // File does not exist
        try
        {
            // create an index file
            file = new BlobFile(indexName,true);
            rootPageNum = 2;
            // Alloc a new page
            Page* headerPage;
            bufMgr -> allocPage(file, headerPageNum, headerPage);
            IndexMetaInfo *metaPage = (IndexMetaInfo*)headerPage;
            // Store data into header page
            strcpy(metaPage -> relationName, relationName.c_str());
            metaPage -> attrByteOffset = attrByteOffset;
            metaPage -> attrType = attrType;
            metaPage -> rootPageNo = 2;
            bufMgr -> unPinPage(file, headerPageNum, true);
            // Create a FileScan object to obtain records from relation
            FileScan fc(relationName, bufMgr);
            // Create the root page
            try
            {
                RecordId scanRid;
                // get the first record and create a root
                fc.scanNext(scanRid);
                std::string recordStr = fc.getRecord();
                const char *record = recordStr.c_str();
                int key = *((int *)(record + offsetof (RECORD, i)));
                Page *rootPage;
                bufMgr -> allocPage(file,rootPageNum,rootPage);
                LeafNodeInt* rootNode = (LeafNodeInt*)rootPage;
                rootNode -> keyArray[0] = key;
                rootNode -> ridArray[0] = scanRid;
                bufMgr -> unPinPage(file, rootPageNum, true);
                // Get all the records from the relation
                while (1)
                {
                    fc.scanNext(scanRid);
                    std::string recordStr = fc.getRecord();
                    const char *record = recordStr.c_str();
                    int key = *((int *)(record + offsetof (RECORD, i)));
                    insertEntry(&key,scanRid);
                }
            }
            // Hit the end
            catch (EndOfFileException e)
            {
                bufMgr -> flushFile(file);
            }
        }
        // File exists
        catch (FileExistsException e)
        {
            // open && read an existing file
            file = new BlobFile(indexName,false);
            Page* headerPage;
            bufMgr -> readPage(file, headerPageNum, headerPage);
            IndexMetaInfo* metaPage = (IndexMetaInfo*)headerPage;
            rootPageNum = metaPage -> rootPageNo;
            // The the data of metaPage does not match the initial one
            if (relationName != metaPage -> relationName ||
                         attrByteOffset != metaPage -> attrByteOffset || attrType != metaPage -> attrType)
            {
                throw BadIndexInfoException(outIndexName);
            }
            bufMgr -> unPinPage(file, headerPageNum, true);
        }
    }
/**
 * BTreeIndex Destructor.
 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
 * and delete file instance thereby closing the index file.
 * Destructor should not throw any exceptions. All exceptions should be caught in here itself.
 **/
    BTreeIndex::~BTreeIndex()
    {
        scanExecuting = false;
        bufMgr -> flushFile(BTreeIndex::file);
        delete file;
        file = nullptr;
    }
/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 * @param key Key to insert, pointer to integer/double/char string
 * @param rid Record ID of a record whose entry is getting inserted into the index.
**/
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
        RIDKeyPair<int> pair;
        pair.set(rid,*((int*)key));
        // If the root is leaf node
        if (rootPageNum == 2)
        {
            insert(pair, rootPageNum, 1);
        }
        // If the root is non-leaf node
        else
        {
            insert(pair, rootPageNum, 0);
        }
    }
/**
 * Recersivly insert entry into file
 * @param Page* currPage current page we check
 * @param PageID currPage
 * @param RIDKeyPair<int> pair the entry insert
 * @param PageKetPair<int>
 */
    PageKeyPair<int>* BTreeIndex::insert(RIDKeyPair<int> pair, PageId currNum, int isLeaf)
    {
        Page* currPage;
        bufMgr -> readPage(file, currNum, currPage);
        // Current node is not leaf
        if (isLeaf == 0)
        {
            NonLeafNodeInt* nonLeaf = (NonLeafNodeInt*) currPage;
            PageKeyPair<int>* pagePairTmp = nullptr;
            // find the child node to insert
            for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
            {
                if (i < INTARRAYNONLEAFSIZE - 1)
                {
                    // compare the insert key value and the first kay value in the nonLeaf node
                    if (i == 0 && nonLeaf -> keyArray[i] > pair.key)
                    {
                        pagePairTmp = insert(pair, nonLeaf -> pageNoArray[i], nonLeaf -> level);
                        break;
                    }
                    // the insert key >= precious key && < next key
                    else if (nonLeaf -> keyArray[i] <= pair.key && pair.key < nonLeaf -> keyArray[i + 1])
                    {
                        pagePairTmp = insert(pair, nonLeaf -> pageNoArray[i+1], nonLeaf -> level);
                        break;
                    }
                    // at the last index of the keyarray && insert key > curr key
                    else if (nonLeaf -> keyArray[i + 1] == 0 && nonLeaf -> keyArray[i] <= pair.key)
                    {
                        pagePairTmp = insert(pair, nonLeaf -> pageNoArray[i+1], nonLeaf -> level);
                        break;
                    }
                }
                // i == INTARRAYNONLEAFSIZE - 1
                else
                {
                    // insert key >= the last key
                    if (nonLeaf -> keyArray[i] <= pair.key)
                    {
                        pagePairTmp = insert(pair, nonLeaf -> pageNoArray[i + 1], nonLeaf -> level);
                        break;
                    }
                }
            }
            // check if child insert moves up the middle key
            if (pagePairTmp != nullptr)
            {
                // if current node has space
                if (nonLeaf -> pageNoArray[INTARRAYNONLEAFSIZE] == 0)
                {
                    insert_nonleaf(*pagePairTmp, *pagePairTmp, nonLeaf);
                    bufMgr -> unPinPage(file, currNum, true);
                    return nullptr;
                }
                // if current node has no space
                else
                {
                    PageKeyPair<int>* moveUpMidPair = split_nonleaf(currNum, nonLeaf, *pagePairTmp);
                    bufMgr -> unPinPage(file, currNum, true);
                    return moveUpMidPair;
                }
            }
            else
            {
                bufMgr -> unPinPage(file, currNum, true);
                return nullptr;
            }
        }
        // if current node is leaf
        else
        {
            LeafNodeInt* leafNode = (LeafNodeInt*) currPage;
            // if current node has space
            if (leafNode -> ridArray[INTARRAYLEAFSIZE - 1].slot_number == 0)
            {
                insert_leaf(pair, leafNode);
                bufMgr -> unPinPage(file, currNum, true);
                return nullptr;
            }
            // if current node has no space
            else
            {
                // split
                PageKeyPair<int>* moveUpMidPair = split_leaf(leafNode, currNum, pair);
                bufMgr -> unPinPage(file, currNum, true);
                return moveUpMidPair;
            }
        }
    }
/**
 * Insert into non-leaf node
 *
 * @param PageKeyPair<int>
 * @param NonleafNode* nonleafnode
 */
    const void BTreeIndex::insert_nonleaf(PageKeyPair<int> pair1, PageKeyPair<int> pair2, NonLeafNodeInt* nonLeafNode)
    {
        // insert into an empty non-leaf node
        if (nonLeafNode -> pageNoArray[0] == 0)
        {
            nonLeafNode -> keyArray[0] = pair2.key;
            nonLeafNode -> pageNoArray[0] = pair1.pageNo;
            nonLeafNode -> pageNoArray[1] = pair2.pageNo;
            return;
        }
        // insert into a non-empty non-leaf node
        PageKeyPair<int> pairContainer = pair2;
        PageKeyPair<int> pairTmp;
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
        {
            if (nonLeafNode -> pageNoArray[i + 1] == 0)
            {
                // insert
                nonLeafNode -> keyArray[i] = pairContainer.key;
                nonLeafNode -> pageNoArray[i + 1] = pairContainer.pageNo;
                break;
            }
            else
            {
                if (nonLeafNode -> keyArray[i] > pair2.key)
                {
                    // save current information, insert pair information into current location
                    pairTmp.key = nonLeafNode->keyArray[i];
                    pairTmp.pageNo = nonLeafNode->pageNoArray[i + 1];
                    nonLeafNode -> keyArray[i] = pairContainer.key;
                    nonLeafNode -> pageNoArray[i + 1] = pairContainer.pageNo;
                    pairContainer = pairTmp;
                }
            }
        }
    }
/**
 * Insert into leaf node
 *
 * @param RIDKeyPair<int> pair
 * @param leafNode* leafnode
 */
    const void BTreeIndex::insert_leaf(RIDKeyPair<int> pair, LeafNodeInt* leafNode)
    {
        RIDKeyPair<int> pairContainer = pair;
        RIDKeyPair<int> pairTmp;
        for (int i = 0; i < INTARRAYLEAFSIZE; i++)
        {
            if (leafNode -> ridArray[i].slot_number == 0)
            {
                // insert
                leafNode -> keyArray[i] = pairContainer.key;
                leafNode -> ridArray[i] = pairContainer.rid;
                break;
            }
            else
            {
                if (leafNode -> keyArray[i] > pair.key)
                {
                    // save current information, insert pair information into current location
                    pairTmp.key = leafNode -> keyArray[i];
                    pairTmp.rid = leafNode -> ridArray[i];
                    leafNode -> keyArray[i] = pairContainer.key;
                    leafNode -> ridArray[i] = pairContainer.rid;
                    pairContainer = pairTmp;
                }
            }
        }
    }
/**
 * Split leaf node
 *
 * @param leafNode* leafNode
 * @param PageId currNum
 * @param RIDKeyPair<int> pair
 * @return PageKeyPair<int>*
 **/
    PageKeyPair<int>* BTreeIndex::split_leaf(LeafNodeInt* leafNode, PageId currNum, RIDKeyPair<int> pair)
    {
        // create a new leaf
        Page* newSibling;
        PageId newSiblingNum;
        bufMgr -> allocPage(file, newSiblingNum, newSibling);
        LeafNodeInt* siblingNode = (LeafNodeInt*) newSibling;
        // add rightSibPageNo to the current leaf node
        if (leafNode -> rightSibPageNo != 0)
        {
            siblingNode -> rightSibPageNo = leafNode -> rightSibPageNo;
        }
        leafNode -> rightSibPageNo = newSiblingNum;
        int cnt = 0;
        // split the current leaf into two leaves
        for (int i = 0; i < INTARRAYLEAFSIZE / 2; i++)
        {
            siblingNode -> keyArray[i] = leafNode -> keyArray[i + INTARRAYLEAFSIZE / 2];
            leafNode -> keyArray[i + INTARRAYLEAFSIZE / 2] = 0;
            siblingNode -> ridArray[i] = leafNode -> ridArray[i + INTARRAYLEAFSIZE / 2];
            leafNode -> ridArray[i + INTARRAYLEAFSIZE / 2].page_number = 0;
            leafNode -> ridArray[i + INTARRAYLEAFSIZE / 2].slot_number = 0;
            cnt++;
        }
        // insert the pair into new splitted leaves
        // insert into the left leaf
        if (pair.key < siblingNode -> keyArray[0])
        {
            insert_leaf(pair, leafNode);
        }
            //   insert into the sibling leaf
        else
        {
            insert_leaf(pair, siblingNode);
        }
        // generate the new mid key pair
        PageKeyPair<int>* left_pair = new PageKeyPair<int>;
        PageKeyPair<int>* right_pair = new PageKeyPair<int>;
        left_pair -> set(currNum, siblingNode -> keyArray[0]);
        right_pair -> set(newSiblingNum, siblingNode -> keyArray[0]);
        return moveUpPair(left_pair, right_pair, 1, newSiblingNum, currNum);
    }
/**
 * Split non-leaf node
 *
 * @param PageId currNum
 * @param NonLeafNodeInt* nonLeafNode
 * @param PageKeyPair pair
 * @return  PageKeyPair<int>*
 */
    PageKeyPair<int>* BTreeIndex::split_nonleaf(PageId currNum, NonLeafNodeInt* nonLeafNode, PageKeyPair<int> pair)
    {
        // create a new non-leaf node
        Page* newSibling;
        PageId newSiblingNum;
        bufMgr -> allocPage(file, newSiblingNum, newSibling);
        NonLeafNodeInt* siblingNode = (NonLeafNodeInt*) newSibling;
        siblingNode -> level = nonLeafNode -> level;
        // split the current non-leaf node to two non-leaf nodes
        for (int i = 0; i < INTARRAYNONLEAFSIZE / 2; i++)
        {
            siblingNode -> keyArray[i] = nonLeafNode -> keyArray[i+INTARRAYNONLEAFSIZE / 2 + 1];
            nonLeafNode -> keyArray[i+INTARRAYNONLEAFSIZE / 2 + 1] = 0;
            siblingNode -> pageNoArray[i] = nonLeafNode -> pageNoArray[i +INTARRAYNONLEAFSIZE / 2 + 1];
            nonLeafNode -> pageNoArray[i+INTARRAYNONLEAFSIZE / 2 + 1] = 0;
        }
        siblingNode -> pageNoArray[INTARRAYNONLEAFSIZE / 2] = nonLeafNode -> pageNoArray[ INTARRAYNONLEAFSIZE];
        nonLeafNode -> pageNoArray[INTARRAYNONLEAFSIZE] = 0;
        int midKey = nonLeafNode -> keyArray[INTARRAYNONLEAFSIZE / 2];
        nonLeafNode -> keyArray[INTARRAYNONLEAFSIZE / 2] = 0;
        // insert the key pair into the new nodes
        // insert into the left non-leaf node
        if (pair.key < siblingNode -> keyArray[0])
        {
            insert_nonleaf(pair, pair, nonLeafNode);
        }
        // insert into the right non-leaf node
        else
        {
            insert_nonleaf(pair, pair, siblingNode);
        }
        PageKeyPair<int>* left_pair = new PageKeyPair<int>;
        PageKeyPair<int>* right_pair = new PageKeyPair<int>;
        left_pair -> set(currNum, midKey);
        right_pair -> set(newSiblingNum, midKey);
        return moveUpPair(left_pair, right_pair, 0, newSiblingNum, currNum);
    }
    /**
     * Get the key that need to be moved up
     *
     * @param leftPair
     * @param rightPair
     * @param level
     * @param newSiblingNum
     * @param currNum
     * @return PageKeyPair<int>*
     */
    PageKeyPair<int>* BTreeIndex::moveUpPair(PageKeyPair<int>* leftPair, PageKeyPair<int>* rightPair, int level, PageId newSiblingNum, PageId currNum)
    {
        if (currNum == rootPageNum)
        {
            Page* newRoot;
            PageId newRootNum;
            bufMgr -> allocPage(file, newRootNum, newRoot);
            NonLeafNodeInt* newRootNode = (NonLeafNodeInt*) newRoot;
            newRootNode -> level = level;
            // insert the key of the new leaves to the new root
            insert_nonleaf(*leftPair, *rightPair, newRootNode);
            bufMgr -> unPinPage(file, newRootNum, true);
            bufMgr -> unPinPage(file, newSiblingNum, true);
            changeRootNum(newRootNum);
            return nullptr;
        }
        // non-root node need to be split, then return the mid key directly to the upper level
        else
        {
            bufMgr -> unPinPage(file, newSiblingNum, true);
            return rightPair;
        }
    }
    /**
     * Change the root
     *
     * @param newRootNum
     */
    const void BTreeIndex::changeRootNum(PageId newRootNum)
    {
        rootPageNum = newRootNum;
        Page* headerPage;
        bufMgr -> readPage(file, headerPageNum, headerPage);
        IndexMetaInfo* headerNode = (IndexMetaInfo*)headerPage;
        headerNode -> rootPageNo = newRootNum;
        bufMgr -> unPinPage(file, headerPageNum, true);
    }
    /**
      * Begin a filtered scan of the index.  For instance, if the method is called
      * using ("a",GT,"d",LTE) then we should seek all entries with a value
      * greater than "a" and less than or equal to "d".
      * If another scan is already executing, that needs to be ended here.
      * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
      * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
      * @param lowVal	Low value of range, pointer to integer / double / char string
      * @param lowOp		Low operator (GT/GTE)
      * @param highVal	High value of range, pointer to integer / double / char string
      * @param highOp	High operator (LT/LTE)
      * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
      * @throws  BadScanrangeException If lowVal > highval
      * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
    **/
    const void BTreeIndex::startScan(const void* lowValParm,
                                     const Operator lowOpParm,
                                     const void* highValParm,
                                     const Operator highOpParm)
    {
        // Initializing
        lowValInt = *((int*)lowValParm);
        highValInt = *((int*)highValParm);
        // BadOpcodesException
        if (!((lowOpParm == GT || lowOpParm == GTE) && (highOpParm == LT || highOpParm == LTE)))
        {
            throw BadOpcodesException();
        }
        // BadScanrangeException
        if (lowValInt > highValInt)
        {
            throw BadScanrangeException();
        }
        // if another scan is on going, end that scan
        if (scanExecuting)
        {
            endScan();
        }
        // initialize for this scan
        scanExecuting = true;
        // update the operator
        lowOp = lowOpParm;
        highOp = highOpParm;
        // recursively find the exact place to start
        // start from the root
        Page* tmp;
        bufMgr -> readPage(file, rootPageNum, tmp);
        // if root is leaf, recursively through all record of root is enough
        if (rootPageNum == 2)
        {
            LeafNodeInt* rootLeaf = (LeafNodeInt*)tmp;
            bool findKey = search_key_in_leaf(rootLeaf , rootPageNum);
            bufMgr -> unPinPage(file, rootPageNum, false);
            if (!findKey)
            {
                endScan();
                throw NoSuchKeyFoundException();
            }
        }
        // if root is not leaf, recursing through all children of root
        else
        {
            NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
            bool findKey = find_leafnode(root, root -> level);
            bufMgr -> unPinPage(file, rootPageNum, false);
            if (!findKey)
            {
                endScan();
                throw NoSuchKeyFoundException();
            }
        }
        bufMgr -> readPage(file, currentPageNum, tmp);
        currentPageData = tmp;
    }

    const bool BTreeIndex::check_nonleaf(NonLeafNodeInt* nonLeafNode, int index){
        Page* page;
        bufMgr->readPage(file,nonLeafNode -> pageNoArray[index],page);
        NonLeafNodeInt* p = (NonLeafNodeInt*) page;
        bool findKey = find_leafnode(p, p->level);
        bufMgr -> unPinPage(file, nonLeafNode -> pageNoArray[index], false);
        return findKey;
    }

    /**
     * find leaf node
     *
     * @param nonLeafNode
     * @param nextNodeIsLeaf
     * @return bool if find the leaf node
     */
    const bool BTreeIndex::find_leafnode(NonLeafNodeInt* nonLeafNode, int nextNodeIsLeaf)
    {
        // the next node is a nonLeafNode
        if (nextNodeIsLeaf == 0)
        {
            for (int i = 0; i < INTARRAYNONLEAFSIZE - 1;i++)
            {
                if (i < INTARRAYNONLEAFSIZE - 1)
                {
                    if (i == 0 && nonLeafNode -> keyArray[i] > lowValInt)
                    {
                        return check_nonleaf(nonLeafNode, i);
                    }
                    else if (nonLeafNode -> keyArray[i] <= lowValInt && lowValInt < nonLeafNode -> keyArray[i + 1])
                    {
                        return check_nonleaf(nonLeafNode, i + 1);
                    }
                    else if (nonLeafNode -> keyArray[i + 1] == 0 && nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return check_nonleaf(nonLeafNode, i + 1);
                    }
                }
                // i == INTARRAYNONLEAFSIZE - 1
                else
                {
                    // insert key >= the last key
                    if (nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return check_nonleaf(nonLeafNode, i + 1);
                    }
                }
            }
        }
        // the next node is leafnode
        else if (nextNodeIsLeaf == 1)
        {
            for(int i = 0; i < INTARRAYNONLEAFSIZE - 1;i++)
            {
                if (i < INTARRAYNONLEAFSIZE - 1)
                {
                    if( i == 0 && nonLeafNode -> keyArray[i] > lowValInt)
                    {
                        return check_leaf(nonLeafNode, i);
                    }
                    else if (nonLeafNode -> keyArray[i] <= lowValInt && lowValInt < nonLeafNode -> keyArray[i + 1])
                    {
                        return check_leaf(nonLeafNode, i + 1);
                    }
                    else if (nonLeafNode -> keyArray[i + 1] == 0 && nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return check_leaf(nonLeafNode, i + 1);
                    }
                }
                else
                {
                    if (nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return check_leaf(nonLeafNode, i + 1);
                    }
                }
            }
        }
        return false;
    }

    const bool BTreeIndex::check_leaf(NonLeafNodeInt* nonLeafNode, int index) {
        Page* page;
        bufMgr->readPage(file,nonLeafNode -> pageNoArray[index],page);
        LeafNodeInt* p = (LeafNodeInt*) page;
        bool findKey = search_key_in_leaf(p, nonLeafNode -> pageNoArray[index]);
        bufMgr -> unPinPage(file, nonLeafNode -> pageNoArray[index], false);

        return findKey;
    }
// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
    const void BTreeIndex::scanNext(RecordId& outRid)
    {
        // Scan is not initialized
        if (!scanExecuting)
        {
            std::cout << "scan not initialized" << std::endl;
            throw ScanNotInitializedException();
        }
        LeafNodeInt* currNode = (LeafNodeInt*) currentPageData;
        // If the pageNo of next RID == 0 || hit the end of the array
        if (currNode -> ridArray[nextEntry].page_number == 0 || nextEntry == INTARRAYLEAFSIZE)
        {
            bufMgr -> unPinPage(file, currentPageNum, false);
            // If there is no right sibling page
            if (currNode -> rightSibPageNo == 0)
            {
                throw IndexScanCompletedException();
            }
            currentPageNum = currNode -> rightSibPageNo;
            bufMgr -> readPage(file, currentPageNum, currentPageData);
            currNode = (LeafNodeInt*) currentPageData;
            nextEntry = 0;
        }
        int key = currNode -> keyArray[nextEntry];
        // Key is valid (in the desired range)
        if (checkValid(key))
        {
            outRid = currNode -> ridArray[nextEntry];
            nextEntry++;
        }
        // Key is not valid
        else
        {
            bufMgr -> unPinPage(file, currentPageNum, false);
            throw IndexScanCompletedException();
        }
    }
// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan()
    {
        if (!scanExecuting)
        {
            throw ScanNotInitializedException();
        }
        scanExecuting = false;
        currentPageData = nullptr;
        currentPageNum = -1;
        nextEntry = -1;
    }
    const bool BTreeIndex::checkValid(int key)
    {
        if(lowOp == GT && highOp == LT)
        {
            return key > lowValInt && key < highValInt;
        }
        else if(lowOp == GTE && highOp == LT)
        {
            return key >= lowValInt && key < highValInt;
        }
        else if(lowOp == GT && highOp == LTE)
        {
            return key > lowValInt && key <= highValInt;
        }
        else
        {
            return key >= lowValInt && key <= highValInt;
        }
    }
    const bool BTreeIndex::search_key_in_leaf(LeafNodeInt* LeafNode, int PageNum)
    {
        for(int i = 0;i < INTARRAYLEAFSIZE; i++) {
            if (checkValid(LeafNode->keyArray[i])) {
                nextEntry = i;
                currentPageNum = PageNum;
                return true;
            }
        }
        return false;
    }
}