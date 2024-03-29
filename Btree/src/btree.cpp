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

namespace badgerdb
{
    /**
     * BTreeIndex Constructor.
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
     *
     * @param relationName Name of file.
     * @param outIndexName Return the name of index file.
     * @param bufMgrIn Buffer Manager Instance
     * @param attrByteOffset Offset of attribute, over which index is to be built, in the record
     * @param attrType Datatype of attribute over which index is built
     * @throws  BadIndexInfoException If the index file already exists for the corresponding attribute,
     *                     but values in metapage(relationName, attribute byte offset, attribute type etc.)
     *                     do not match with values received through constructor parameters.
     */
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
            IndexMetaInfo* metaPage = (IndexMetaInfo*)headerPage;
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
                Page *rootPage;
                bufMgr -> allocPage(file,rootPageNum,rootPage);
                LeafNodeInt* rootNode = (LeafNodeInt*)rootPage;
                rootNode -> keyArray[0] = *((int*)record + attrByteOffset);
                rootNode -> ridArray[0] = scanRid;
                bufMgr -> unPinPage(file, rootPageNum, true);
                // Get all the records from the relation
                while (1)
                {
                    fc.scanNext(scanRid);
                    std::string recordStr = fc.getRecord();
                    const char *record = recordStr.c_str();
                    insertEntry(record + attrByteOffset, scanRid);
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
     */
    BTreeIndex::~BTreeIndex()
    {
        scanExecuting = false;
        bufMgr -> flushFile(file);
        delete file;
        file = nullptr;
    }
    /**
     * Insert a new entry using the pair <value,rid>.
     * Start from root to recursively find out the leaf to insert the entry in.
     * The insertion may cause splitting of leaf node.
     * This splitting will require addition of new leaf page number entry into the parent non-leaf,
     * which may in-turn get split.
     * This may continue all the way upto the root causing the root to get split.
     * If root gets split, metapage needs to be changed accordingly.
     * Make sure to unpin pages as soon as you can.
     *
     * @param key Key to insert, pointer to integer/double/char string
     * @param rid Record ID of a record whose entry is getting inserted into the index.
     */
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
        RIDKeyPair<int> pair;
        pair.set(rid, *((int*)key));
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
     * Begin a filtered scan of the index.  For instance, if the method is called
     * using ("a",GT,"d",LTE) then we should seek all entries with a value
     * greater than "a" and less than or equal to "d".
     * If another scan is already executing, that needs to be ended here.
     * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
     * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
     *
     * @param lowVal	Low value of range, pointer to integer / double / char string
     * @param lowOp		Low operator (GT/GTE)
     * @param highVal	High value of range, pointer to integer / double / char string
     * @param highOp	High operator (LT/LTE)
     * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
     * @throws  BadScanrangeException If lowVal > highval
     * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
     */
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
        bool findKey = false;
        // if root is leaf, recursively through all record of root is enough
        if (rootPageNum == 2)
        {
            LeafNodeInt* rootLeaf = (LeafNodeInt*)tmp;
            findKey = searchKeyInLeaf(rootLeaf, rootPageNum);
        }
        // if root is not leaf, recursing through all children of root
        else
        {
            NonLeafNodeInt* root = (NonLeafNodeInt*)tmp;
            findKey = findLeafNode(root, root -> level);
        }
        bufMgr -> unPinPage(file, rootPageNum, false);
        // does not find key
        if (!findKey)
        {
            endScan();
            throw NoSuchKeyFoundException();
        }
        bufMgr -> readPage(file, currentPageNum, tmp);
        currentPageData = tmp;
    }
    /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety,
     * move on to the right sibling of current page, if any exists, to start scanning that page.
     * Make sure to unpin any pages that are no longer required.
     *
     * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	 */
    const void BTreeIndex::scanNext(RecordId& outRid)
    {
        // Scan is not initialized
        if (!scanExecuting)
        {
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
            // There is valid sibling page, set data
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
    /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
     *
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 */
    const void BTreeIndex::endScan()
    {
        if (!scanExecuting)
        {
            throw ScanNotInitializedException();
        }
        // reset vars
        scanExecuting = false;
        currentPageData = nullptr;
        currentPageNum = -1;
        nextEntry = -1;
    }
    /**
     * Recursively insert entry into file
     *
     * @param pair inserting entry
     * @param currNum current page number
     * @param ifLeaf if a node is leaf
     * @return PageKeyPair<int>*
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
                        pagePairTmp = insert(pair, nonLeaf -> pageNoArray[i + 1], nonLeaf -> level);
                        break;
                    }
                    // at the last index of the keyArray && insert key > curr key
                    else if (nonLeaf -> keyArray[i + 1] == 0 && nonLeaf -> keyArray[i] <= pair.key)
                    {
                        pagePairTmp = insert(pair, nonLeaf -> pageNoArray[i + 1], nonLeaf -> level);
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
                    insertNonLeaf(*pagePairTmp, *pagePairTmp, nonLeaf);
                    bufMgr -> unPinPage(file, currNum, true);
                    return nullptr;
                }
                // if current node has no space
                else
                {
                    PageKeyPair<int>* moveUpMidPair = splitNonLeaf(currNum, nonLeaf, *pagePairTmp);
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
                insertLeaf(pair, leafNode);
                bufMgr -> unPinPage(file, currNum, true);
                return nullptr;
            }
            // if current node has no space
            else
            {
                // split
                PageKeyPair<int>* moveUpMidPair = splitLeaf(leafNode, currNum, pair);
                bufMgr -> unPinPage(file, currNum, true);
                return moveUpMidPair;
            }
        }
    }
    /**
     * Insert into non-leaf node
     *
     * @param pair1 the left pair
     * @param pair2 the right pair
     * @param nonLeafNode current node working on
     */
    const void BTreeIndex::insertNonLeaf(PageKeyPair<int> pair1, PageKeyPair<int> pair2, NonLeafNodeInt *nonLeafNode)
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
     * @param pair the RidKeyPair
     * @param leafNode current node working on
     */
    const void BTreeIndex::insertLeaf(RIDKeyPair<int> pair, LeafNodeInt *leafNode)
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
     * @param leafNode current leaf node
     * @param currNum current page number
     * @param pair the RIDKeyPair<int>
     * @return PageKeyPair<int>*
     */
    PageKeyPair<int>* BTreeIndex::splitLeaf(LeafNodeInt *leafNode, PageId currNum, RIDKeyPair<int> pair)
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
        // split the current leaf into two leaves
        for (int i = 0; i < INTARRAYLEAFSIZE / 2; i++)
        {
            siblingNode -> keyArray[i] = leafNode -> keyArray[i + INTARRAYLEAFSIZE / 2];
            leafNode -> keyArray[i + INTARRAYLEAFSIZE / 2] = 0;
            siblingNode -> ridArray[i] = leafNode -> ridArray[i + INTARRAYLEAFSIZE / 2];
            leafNode -> ridArray[i + INTARRAYLEAFSIZE / 2].page_number = 0;
            leafNode -> ridArray[i + INTARRAYLEAFSIZE / 2].slot_number = 0;
        }
        // insert the pair into new splitted leaves
        // insert into the left leaf
        if (pair.key < siblingNode -> keyArray[0])
        {
            insertLeaf(pair, leafNode);
        }
        // insert into the sibling leaf
        else
        {
            insertLeaf(pair, siblingNode);
        }
        // generate the new mid key pair
        PageKeyPair<int>* leftPair = new PageKeyPair<int>;
        PageKeyPair<int>* rightPair = new PageKeyPair<int>;
        leftPair -> set(currNum, siblingNode -> keyArray[0]);
        rightPair -> set(newSiblingNum, siblingNode -> keyArray[0]);
        return moveUpPair(leftPair, rightPair, 1, newSiblingNum, currNum);
    }
    /**
     * Split non-leaf node
     *
     * @param currNum current page number
     * @param nonLeafNode current node working on
     * @param pair the PageKeyPair
     * @return PageKeyPair<int>*
     */
    PageKeyPair<int>* BTreeIndex::splitNonLeaf(PageId currNum, NonLeafNodeInt *nonLeafNode, PageKeyPair<int> pair)
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
            insertNonLeaf(pair, pair, nonLeafNode);
        }
        // insert into the right non-leaf node
        else
        {
            insertNonLeaf(pair, pair, siblingNode);
        }
        PageKeyPair<int>* leftPair = new PageKeyPair<int>;
        PageKeyPair<int>* rightPair = new PageKeyPair<int>;
        leftPair -> set(currNum, midKey);
        rightPair -> set(newSiblingNum, midKey);
        return moveUpPair(leftPair, rightPair, 0, newSiblingNum, currNum);
    }
    /**
     * Get the key that need to be moved up
     *
     * @param leftPair the pair of left node
     * @param rightPair the pair of right node
     * @param level the level of node to be set
     * @param newSiblingNum the page number of new sibling node
     * @param currNum current page number
     * @return PageKeyPair<int>*
     */
    PageKeyPair<int>* BTreeIndex::moveUpPair(PageKeyPair<int>* leftPair, PageKeyPair<int>* rightPair,
                                                            int level, PageId newSiblingNum, PageId currNum)
    {
        if (currNum == rootPageNum)
        {
            Page* newRoot;
            PageId newRootNum;
            bufMgr -> allocPage(file, newRootNum, newRoot);
            NonLeafNodeInt* newRootNode = (NonLeafNodeInt*) newRoot;
            newRootNode -> level = level;
            // insert the key of the new leaves to the new root
            insertNonLeaf(*leftPair, *rightPair, newRootNode);
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
     * Updating the root
     *
     * @param newRootNum new root page number
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
     * check if a node is non_leaf node
     *
     * @param nonLeafNode
     * @param index
     * @return if needed recursive call
     */
    const bool BTreeIndex::checkNonLeaf(NonLeafNodeInt *nonLeafNode, int index)
    {
        Page* page;
        bufMgr->readPage(file,nonLeafNode -> pageNoArray[index],page);
        NonLeafNodeInt* p = (NonLeafNodeInt*) page;
        bool findKey = findLeafNode(p, p->level);
        bufMgr -> unPinPage(file, nonLeafNode -> pageNoArray[index], false);
        return findKey;
    }
    /**
     * check if node is leaf
     *
     * @param nonLeafNode
     * @param index
     * @return if need recursive call
     */
    const bool BTreeIndex::checkLeaf(NonLeafNodeInt *nonLeafNode, int index)
    {
        Page* page;
        bufMgr->readPage(file,nonLeafNode -> pageNoArray[index],page);
        LeafNodeInt* p = (LeafNodeInt*) page;
        bool findKey = searchKeyInLeaf(p, nonLeafNode->pageNoArray[index]);
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
    const bool BTreeIndex::findLeafNode(NonLeafNodeInt *nonLeafNode, int nextNodeIsLeaf)
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
                        return checkNonLeaf(nonLeafNode, i);
                    }
                    else if (nonLeafNode -> keyArray[i] <= lowValInt && lowValInt < nonLeafNode -> keyArray[i + 1])
                    {
                        return checkNonLeaf(nonLeafNode, i + 1);
                    }
                    else if (nonLeafNode -> keyArray[i + 1] == 0 && nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return checkNonLeaf(nonLeafNode, i + 1);
                    }
                }
                // i == INTARRAYNONLEAFSIZE - 1
                else
                {
                    // insert key >= the last key
                    if (nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return checkNonLeaf(nonLeafNode, i + 1);
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
                        return checkLeaf(nonLeafNode, i);
                    }
                    else if (nonLeafNode -> keyArray[i] <= lowValInt && lowValInt < nonLeafNode -> keyArray[i + 1])
                    {
                        return checkLeaf(nonLeafNode, i + 1);
                    }
                    else if (nonLeafNode -> keyArray[i + 1] == 0 && nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return checkLeaf(nonLeafNode, i + 1);
                    }
                }
                else
                {
                    if (nonLeafNode -> keyArray[i] <= lowValInt)
                    {
                        return checkLeaf(nonLeafNode, i + 1);
                    }
                }
            }
        }
        return false;
    }
    /**
     * Check if the key is valid
     *
     * @param key
     * @return checking result
     */
    const bool BTreeIndex::checkValid(int key)
    {
        if (lowOp == GT && highOp == LT)
        {
            return key > lowValInt && key < highValInt;
        }
        else if (lowOp == GTE && highOp == LT)
        {
            return key >= lowValInt && key < highValInt;
        }
        else if (lowOp == GT && highOp == LTE)
        {
            return key > lowValInt && key <= highValInt;
        }
        else
        {
            return key >= lowValInt && key <= highValInt;
        }
    }
    /**
     * Searching key in the given leaf node
     *
     * @param LeafNode
     * @param PageNum
     * @return
     */
    const bool BTreeIndex::searchKeyInLeaf(LeafNodeInt *LeafNode, int PageNum)
    {
        for (int i = 0;i < INTARRAYLEAFSIZE; i++)
        {
            // key is valid
            if (checkValid(LeafNode->keyArray[i]))
            {
                nextEntry = i;
                currentPageNum = PageNum;
                return true;
            }
        }
        return false;
    }
}