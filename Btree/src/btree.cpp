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
	// construct an index file name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();
	std::cout << indexName << std::endl;
	outIndexName = indexName;

	try
	{
		// create an index file
		file = new BlobFile(indexName,true);
		std::cout << "I created BlobFile object" << std::endl;

		// initialize related private fields
		bufMgr = bufMgrIn;
		headerPageNum = 1;
		rootPageNum = 2;
		this -> attrByteOffset = attrByteOffset;
		leafOccupancy = 0;
		nodeOccupancy = 0;

		Page* headerPage;

		bufMgr -> allocPage(file, headerPageNum, headerPage);
		std::cout << "headerPageNum = " << headerPageNum << std::endl;
		IndexMetaInfo *metaPage = (IndexMetaInfo*)headerPage;
		strcpy(metaPage -> relationName, relationName.c_str());
		std::cout << "relation name = "<< metaPage -> relationName << std::endl;
		metaPage -> attrByteOffset = attrByteOffset;
		std::cout << "attrByteOffset = " << metaPage -> attrByteOffset << std::endl;
		metaPage -> attrType = attrType;
		std::cout << "attrType = " << metaPage -> attrType << std::endl;
		metaPage -> rootPageNo = 2;
		bufMgr -> unPinPage(file, headerPageNum, true);

		// test for the header page
        // std::cout << "the header page is at page number " <<  headerPageNum << std::endl;
		// Page* headerPageTest;
		// bufMgr->readPage(file, headerPageNum, headerPageTest);
		// IndexMetaInfo* headerTest = (IndexMetaInfo*)headerPageTest;
		// std::cout << headerTest->relationName << std::endl;
		// std::cout << headerTest->attrByteOffset << std::endl;
		// std::cout << headerTest->attrType << std::endl;
		// std::cout << headerTest->rootPageNo << std::endl;
		// bufMgr->unPinPage(file, headerPageNum, false);


		FileScan fc(relationName, bufMgr);
		std::cout << "I created FileScan object with relation name " << relationName << std::endl;
		try
		{
			RecordId scanRid;

			// get the first record and create a root
			fc.scanNext(scanRid);
			//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
			std::string recordStr = fc.getRecord();
			const char *record = recordStr.c_str();
			int key = *((int *)(record + offsetof (RECORD, i)));
			std::cout << "the key to insert in root is " << key << std::endl;
			std::cout << "the rid to insert in root is " << scanRid.page_number << " " << scanRid.slot_number << std::endl;

			Page *rootPage;
			bufMgr -> allocPage(file,rootPageNum,rootPage);
			std::cout << "the root page number is " << rootPageNum << std::endl;	
			LeafNodeInt* rootNode = (LeafNodeInt*)rootPage;
			rootNode -> keyArray[0] = key;
			rootNode -> ridArray[0] = scanRid;
			//rootPage.rightSibPageNo = NULL; 
			//bufMgr -> unPinPage(file, rootPageNum, true);

			// test for the header page
        	//std::cout << "the header page is at page number " <<  rootPageNum << std::endl;
			//Page* rootPageTest;
			//bufMgr -> readPage(file, rootPageNum, rootPageTest);
			//LeafNodeInt* rootTest = (LeafNodeInt*)rootPageTest;
			// std::cout << sizeof(rootTest->keyArray) << std::endl;
			// std::cout << rootTest->ridArray[0].page_number << " " << rootTest->ridArray[0].slot_number << std::endl;
			bufMgr -> unPinPage(file, rootPageNum, true);

			while(1)
			//for (int i = 0; i < 682; i++)
			{
				// std::cout << "while" << std::endl;
				fc.scanNext(scanRid);
				// std::cout << "scanNext" << std::endl;
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
				std::string recordStr = fc.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int *)(record + offsetof (RECORD, i)));
				// std::cout << "mm Extracted : " << key << " " << scanRid.page_number << " " << scanRid.slot_number << std::endl;
				insertEntry(&key,scanRid);
			}
		}
		catch (EndOfFileException e)
		{
			std::cout << "put all records" << std::endl;
			bufMgr -> flushFile(file);
		}

		// test, print out record in one leaf
		Page* testPage;
		bufMgr -> readPage(file, rootPageNum, testPage);
		LeafNodeInt* testNode = (LeafNodeInt*)testPage;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++)
		{
			if(testNode -> ridArray[i].slot_number == 0) 
			{
				break;
			}
			std::cout << testNode -> keyArray[i] << " " << testNode -> ridArray[i].page_number << " " << testNode -> ridArray[i].slot_number << std::endl;  
		}
		bufMgr -> unPinPage(file, rootPageNum, false);
		std::cout << "unpinned" << std::endl;
		printOutAllTree();
	}
	catch (FileExistsException e)
	{
		// open an existing file
		file = new BlobFile(indexName,false);
		std::cout << "catch file exist exception" << std::endl;

		// initialize related private fields
		bufMgr = bufMgrIn;
		headerPageNum = 1;
		this -> attrByteOffset = attrByteOffset;
		leafOccupancy = 0;
		nodeOccupancy = 0;

		Page* headerPage;
		std::cout << "file exist, headernum = " << headerPageNum << std::endl;
		bufMgr -> readPage(file, headerPageNum, headerPage);
		IndexMetaInfo* metaPage = (IndexMetaInfo*)headerPage;
		std::cout << "attrtype meta " << metaPage->attrType << std::endl;
		std::cout << "atroffest meta " << metaPage->attrByteOffset << std::endl;
		std::cout << "ralationName meta " << metaPage->relationName << std::endl;
		rootPageNum = metaPage -> rootPageNo;
		bufMgr -> unPinPage(file, headerPageNum, true);
		std::cout << "the rootPageNum now is " << rootPageNum << std::endl;

		printOutAllTree();
	}
	catch(const std::exception& ex)
	{
		// speciffic handling for all exceptions extending std::exception, except
		// std::runtime_error which is handled explicitly
		std::cerr << "Error occurred: " << ex.what() << std::endl;
	}	

}

void BTreeIndex::printOutAllTree()
{
	Page* tmp;
	bufMgr -> readPage(file, rootPageNum, tmp);
	PageId tmpNum = rootPageNum;

	// if root page is leaf
	if(rootPageNum == 2)
	{
		bufMgr -> unPinPage(file, rootPageNum, false); 
		printThisLeft(rootPageNum);
	}
	// if root page is not leaf
	else
	{
		while(1)
		{
			PageId nextPage;
			NonLeafNodeInt* tmpNoneLeaf = (NonLeafNodeInt*)tmp;
			//if children are leaves, print out each child page 
			if(tmpNoneLeaf -> level == 1)
			{
				std::cout << "I am in lalalalalalalllllllllllllllllllllllllllllllllllllllllllllll" << std::endl;
				for(int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++)
				{
					if(tmpNoneLeaf -> pageNoArray[i] == 0)
					{
						break;
					}	
					printThisLeft(tmpNoneLeaf -> pageNoArray[i]);
					nextPage = tmpNoneLeaf -> pageNoArray[i];
				}
				bufMgr -> unPinPage(file, tmpNum, false);
				Page* lalala;
				bufMgr -> readPage(file, nextPage, lalala);
				LeafNodeInt* lalaLeaf = (LeafNodeInt*)lalala;
				std::cout << "before the while" << lalaLeaf -> rightSibPageNo << std::endl;
				bufMgr -> unPinPage(file, nextPage, false);
				PageId rightNum = lalaLeaf -> rightSibPageNo;
				while(rightNum > 0)
				{
					std::cout << "lalaLeaf > 0" << std::endl;
					std::cout << "rightNum " << rightNum << std::endl;
					printThisLeft(rightNum);
					bufMgr -> readPage(file, rightNum, lalala);
					lalaLeaf = (LeafNodeInt*)lalala;
					std::cout << lalaLeaf -> rightSibPageNo << std::endl;
					bufMgr -> unPinPage(file, rightNum, false);
					rightNum = lalaLeaf -> rightSibPageNo;
					//bufMgr -> unPinPage(file, rightNum, false);
				}

				break;
			}
			// if children are non leaves, check the leftMostChild
			else if(tmpNoneLeaf -> level == 0)
			{
				PageId leftMost = tmpNoneLeaf -> pageNoArray[0];
				if(leftMost == 0)
				{
					std::cout << "THIS SHOULD NEVER PRINT:::::::::::::::::::::::There is something wrong" << std::endl;
				}
				else
				{
					bufMgr -> unPinPage(file, tmpNum, false);
					tmpNum = leftMost;
					bufMgr -> readPage(file, tmpNum, tmp);
				}
			}
			else
			{
				std::cout << "THIS SHOULD NEVER PRINT:::::::::::::::::the level of this node is " << tmpNoneLeaf -> level << std::endl;
				break;
			}
		}
	}
}

void BTreeIndex::printThisLeft(PageId tmpNo){
	std::cout << "ready to print" << std::endl;
	Page* tmpPage;
	bufMgr -> readPage(file, tmpNo, tmpPage);
	LeafNodeInt* leafNode = (LeafNodeInt*)tmpPage;
	for(int i = 0; i < INTARRAYLEAFSIZE; i++)
	{
		if(leafNode -> ridArray[i].slot_number != 0)
		{
			std::cout << leafNode -> keyArray[i] << " ";
		}
	}
	std::cout << std::endl;
	bufMgr -> unPinPage(file, tmpNo, false);
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	std::cout << "in the destructor" << std::endl;
	bufMgr -> flushFile(file);
	//file -> close();
	std::cout << "flushed" << std::endl;
	//file -> ~File();
	delete file;
	std::cout << "deleted" << std::endl;
	file = NULL;
	std::cout << "nulled" << std::endl;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
 * Insert a new entry using pair <rid, key>
 * Insertion might lead to the spitting in leaf node, spitting leaf node might cause
 * non-leaf node spitting
 * If the root need to be spit, the metapage need to be updated
 * 
 */
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	RIDKeyPair<int> pair;
	pair.set(rid,*((int*)key));
	// if the node is not full
	//insert_leaf(pair, rootPageNum);
	if (rootPageNum == 2) 
	{
		// std::cout << "in if" << std::endl;
		insert(pair, rootPageNum, 1);		
	}
	else 
	{
		// std::cout << "in else" << std::endl;
		insert(pair, rootPageNum, 0);
	}
	// find leaf
	// whether full
	// 1 -> re
	// 0 -> insert

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
	// if current node is not leaf
	if (isLeaf == 0) 
	{
		NonLeafNodeInt* nonleaf = (NonLeafNodeInt*) currPage;
		PageKeyPair<int>* pagePairTmp = NULL;
		int validEndPageIndex = -1;
		// find the child node to insert
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) 
		{
			if(i < INTARRAYNONLEAFSIZE - 1)
   			{
				// compare the insert key value and the first kay value in the nonleaf node
    			if(i == 0 && nonleaf -> keyArray[i] > pair.key)
    			{
     				pagePairTmp = insert(pair, nonleaf -> pageNoArray[i], nonleaf -> level);
     				break;
    			}
				// the insert key >= precious key && < next key
    			else if(nonleaf -> keyArray[i] <= pair.key && pair.key < nonleaf -> keyArray[i+1] )
    			{
     				pagePairTmp = insert(pair, nonleaf -> pageNoArray[i+1], nonleaf -> level);
     				break;
    			}
				// at the last index of the keyarray && insert key > curr key
    			else if(nonleaf -> keyArray[i+1] == 0 && nonleaf -> keyArray[i] <= pair.key)
    			{
     				pagePairTmp = insert(pair, nonleaf -> pageNoArray[i+1], nonleaf -> level);
     				break;
    			}
   			}
			// i == INTARRAYNONLEAFSIZE - 1
   			else
   			{
				// insert key >= the last key
    			if(nonleaf -> keyArray[i] <= pair.key)
    			{
     				pagePairTmp = insert(pair, nonleaf -> pageNoArray[i+1], nonleaf -> level);
     				break;
    			}
   			}
			// if(nonleaf -> pageNoArray[i] == 0){
			// 	break;
			// }

			// within the range of INTARRAYNONLEAFSIZE, if an invalid pageNo is reached, save the index before as ending index
			// if(nonleaf -> pageNoArray[i] != 0){
			// 	validEndPageNoIndex = i - 1;
			// 	break;
			// }

			// // find a key which is larger than the pair.key, insert into its left child
			// if (nonleaf -> keyArray[i] > pair.key) 
			// {
			// 	// std::cout << "insert if" << std::endl;
			// 	pagePairTmp = insert(pair, nonleaf -> pageNoArray[i], nonleaf -> level);
			// 	// std::cout << "insert if after return" << std::endl;

			// 	break;
			// }

			// if the key is larger than all valid key, insert into the right child of the right most valid key
			// if ( nonleaf -> pageNoArray[i+1] != 0 && nonleaf -> keyArray[i] <= pair.key) 
			// {
			// 	// std::cout << "insert else" << std::endl;
			// 	// std::cout << "nonleaf parray last ele " << nonleaf-> pageNoArray[i + 1] << std::endl;
			// 	pagePairTmp = insert(pair, nonleaf-> pageNoArray[i + 1], nonleaf -> level);
			// 	// std::cout << "insert else after return" << std::endl;
			// 	break;
			// }
		}



		// check if child insert moves up the middle key
		if (pagePairTmp != NULL)
		{
			// std::cout << "pagePairTmp != NULL" << std::endl;
			// if current node has space
			if (nonleaf -> pageNoArray[INTARRAYNONLEAFSIZE] == 0)
			{
				// std::cout << "in if at LINE: " << __LINE__ << std::endl;
				insert_nonleaf(*pagePairTmp, *pagePairTmp, nonleaf);
				bufMgr -> unPinPage(file, currNum, true);
				return NULL;
			}
			// if current node has no space
			else
			{
				// std::cout << "in else at LINE: " << __LINE__ << std::endl;
				PageKeyPair<int>* moveUpMidPair = split_nonleaf(currNum, nonleaf, *pagePairTmp);
				bufMgr -> unPinPage(file, currNum, true);
				return moveUpMidPair;
			}
		}
		else
		{
			// std::cout << "pagePairTmp == NULL" << std::endl;
			bufMgr -> unPinPage(file, currNum, true);
			return NULL;
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
			return NULL;
		}
		// if current node has no space
		else 
		{
			// split
			// std::cout << "leaf noed ready to be splited" << std::endl;
			PageKeyPair<int>* moveUpMidPair = split_leaf(leafNode, currNum, pair);
	
			// std::cout << "leaf already splited" << std::endl;
			bufMgr -> unPinPage(file, currNum, true);
			// std::cout << "splited and unpinned" << std::endl;
			return moveUpMidPair;
		}
	}
}


/**
 * @param PageKeyPair<int>  
 * @param NonleafNode* nonleafnode 
 * 
 */
const void BTreeIndex::insert_nonleaf(PageKeyPair<int> pair1, PageKeyPair<int> pair2, NonLeafNodeInt* nonLeafNode)
{
	//Page* page;
	//bufMgr -> readPage(file, page_num, page);
	//NonLeafNodeInt* nonLeafNode = (NonLeafNodeInt*)page;
	
	// insert into an empty nonleaf node
	if(nonLeafNode -> pageNoArray[0] == 0)
	{
		nonLeafNode -> keyArray[0] = pair2.key;
		nonLeafNode -> pageNoArray[0] = pair1.pageNo;
		nonLeafNode -> pageNoArray[1] = pair2.pageNo;
		// std::cout << "insert into an empty nonleaf node" << std::endl;
		//bufMgr -> unPinPage(file, page_num, true);
		return ;
	}
	
	// insert into a non-empty nonleaf node
	PageKeyPair<int> pairContainer = pair2;
	PageKeyPair<int> pairTmp;
	// std::cout << "decleared two pair in FUNCTION: " << __FUNCTION__ << std::endl;
	for(int i = 0; i < INTARRAYNONLEAFSIZE; i++)
	{
		if(nonLeafNode -> pageNoArray[i] == 0)
		{
			// insert
			nonLeafNode -> keyArray[i] = pairContainer.key;
			nonLeafNode -> pageNoArray[i + 1] = pairContainer.pageNo;
			// std::cout << "insert directly" << std::endl;
			break;
		}
		else
		{
			if(nonLeafNode -> keyArray[i] > pair2.key){
				// save current information, insert pair information into current locatiion
				pairTmp.key = nonLeafNode->keyArray[i];
				pairTmp.pageNo = nonLeafNode->pageNoArray[i];
				nonLeafNode -> keyArray[i] = pairContainer.key;
				nonLeafNode -> pageNoArray[i] = pairContainer.pageNo;
				pairContainer = pairTmp;
			}
		}
	}
	//bufMgr -> unPinPage(file, page_num, true);
}

/**
 * @param RIDKeyPair<int> pair
 * @param leafNode* leafnode 
 */
const void BTreeIndex::insert_leaf(RIDKeyPair<int> pair, LeafNodeInt* leafNode)
{
	//Page* page;
	//bufMgr -> readPage(file, page_num, page);
	//LeafNodeInt* leafNode = (LeafNodeInt*)page;
	RIDKeyPair<int> pairContainer = pair;
	RIDKeyPair<int> pairTmp;

	for(int i = 0; i < INTARRAYLEAFSIZE; i++)
	{
		if(leafNode -> ridArray[i].slot_number == 0)
		{
			// insert
			leafNode -> keyArray[i] = pairContainer.key;
			leafNode -> ridArray[i] = pairContainer.rid;
			//bufMgr -> unPinPage(file, page_num, true);
			break;
		}
		else{
			if(leafNode -> keyArray[i] > pair.key)
			{
				// save current information, insert pair information into current locatiion
				pairTmp.key = leafNode -> keyArray[i];
				pairTmp.rid = leafNode -> ridArray[i];
				leafNode -> keyArray[i] = pairContainer.key;
				leafNode -> ridArray[i] = pairContainer.rid;
				pairContainer = pairTmp;
			}
		}
	}
	//bufMgr -> unPinPage(file, page_num, true);
}

/**
 * @param leafNode* leafnode
 * @param RIDKeyPair<int> pair
 * @param Nonleafnode* parent
 * @param PageKeyPair<int>&
 */
PageKeyPair<int>* BTreeIndex::split_leaf(LeafNodeInt* leafNode, PageId currNum, RIDKeyPair<int> pair)
{
	// create a new leaf
	Page* newSibling;
	PageId newSiblingNum;
	bufMgr -> allocPage(file, newSiblingNum, newSibling);
	LeafNodeInt* siblingNode = (LeafNodeInt*) newSibling;
	// add rightSibPageNo to the current leaf node
	leafNode -> rightSibPageNo = newSiblingNum;
	int cnt = 0;
	// std::cout << "decleared vars in " << __FUNCTION__ << std::endl;
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
	// std::cout << "split " << cnt << " times" << std::endl;
	// insert the pair into new splitted leaves
	// insert into the left leaf
	if (pair.key < siblingNode -> keyArray[0])
	{
		insert_leaf(pair, leafNode);
		// std::cout << "insert leaf into left" << std::endl;
	}
	//   insert into the sibling leaf
	else 
	{
		insert_leaf(pair, siblingNode);
		// std::cout << "insert leaf into right" << std::endl;
	}

	// generate the new mid key pair
	PageKeyPair<int>* left_pair = new PageKeyPair<int>;
	PageKeyPair<int>* right_pair = new PageKeyPair<int>;
	left_pair -> set(currNum, siblingNode -> keyArray[0]);
	right_pair -> set(newSiblingNum, siblingNode -> keyArray[0]);
	// std::cout <<"in function " << __FUNCTION__ << "at line " << __LINE__ << " set two pair" << std::endl;
	
	// if the leaf to be split is root
	if (currNum == rootPageNum) 
	{
		// std::cout << "split root leaf" << std::endl;
		Page* newRoot;
		PageId newRootNum;
		bufMgr -> allocPage(file, newRootNum, newRoot);
		NonLeafNodeInt* newRootNode = (NonLeafNodeInt*) newRoot;
		newRootNode -> level = 1;
		
		// insert the key of the new splitted leaves to the new root
		insert_nonleaf(*left_pair, *right_pair, newRootNode);
		// std::cout << "insert the key of the new splitted leaves to the new root" << std::endl;
		bufMgr -> unPinPage(file, newRootNum, true);
		bufMgr -> unPinPage(file, newSiblingNum, true);

		changeRootNum(newRootNum);
		std::cout << "the new root number is " << rootPageNum << std::endl;
		// bufMgr -> unPinPage(file, newSiblingNum, true);
		return NULL;
	}
	// non-root node need to be splited, then return the mid key directly to the upper level
	else
	{
		// std::cout << "split non-root leaf" << std::endl;
		bufMgr -> unPinPage(file, newSiblingNum, true);
		return right_pair;
	}
	
}

/**
 * @param NonleafNode* nonleafnode
 * @param 
 */
PageKeyPair<int>* BTreeIndex::split_nonleaf(PageId curpagenum, NonLeafNodeInt* nonLeafNode, PageKeyPair<int> pair)
{
	//Page* curpage;
	//bufMgr -> readPage(file, curpagenum, curpage);

	// create a new non-leaf node
	Page* newSibling;
	PageId newSiblingNum;
	// std::cout << "ready to alloc page in FUNCTION: " << __FUNCTION__ << std::endl;
	bufMgr -> allocPage(file, newSiblingNum, newSibling);
	// std::cout << "done alloc page in FUNCTION: " << __FUNCTION__ << std::endl;
	NonLeafNodeInt* siblingNode = (NonLeafNodeInt*) newSibling;
	siblingNode -> level = nonLeafNode -> level;
	// std::cout << "decleared vars in FUNCTION: " << __FUNCTION__ << std::endl;
	// split the current non-leaf node to two non-leaf nodes
	for(int i = 0; i < INTARRAYNONLEAFSIZE / 2; i++)
	{
		siblingNode -> keyArray[i] = nonLeafNode -> keyArray[i+INTARRAYNONLEAFSIZE / 2 + 1];
		nonLeafNode -> keyArray[i+INTARRAYNONLEAFSIZE / 2 + 1] = 0;
		siblingNode -> pageNoArray[i] = nonLeafNode -> pageNoArray[i +INTARRAYNONLEAFSIZE / 2 + 1];
		nonLeafNode -> pageNoArray[i+INTARRAYNONLEAFSIZE / 2 + 1] = 0;
	}
	// std::cout << "done spliting curr non-leaf node in FUNC: " << __FUNCTION__ << std::endl;
	siblingNode -> pageNoArray[INTARRAYNONLEAFSIZE / 2] = nonLeafNode -> pageNoArray[ INTARRAYNONLEAFSIZE];
	nonLeafNode -> pageNoArray[INTARRAYNONLEAFSIZE] = 0;
	//PageKeyPair<int> * midpair = new PageKeyPair<int>;
	// std::cout << "key value at nonleafnode size/2: " << nonLeafNode -> keyArray[INTARRAYNONLEAFSIZE/2] << std::endl;
	//midpair -> key = nonLeafNode -> keyArray[INTARRAYNONLEAFSIZE/2];
	//nonLeafNode -> keyArray[INTARRAYNONLEAFSIZE/2] = 0;
	//midpair -> pageNo = newSiblingNum;
	int midKey = nonLeafNode -> keyArray[INTARRAYNONLEAFSIZE / 2];
	nonLeafNode -> keyArray[INTARRAYNONLEAFSIZE / 2] = 0;
	// insert the key pair into the newly splitted nodes
	// insert into the left non-leaf node
	if(pair.key < siblingNode -> keyArray[0])
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
	left_pair -> set(curpagenum, midKey);
	right_pair -> set(newSiblingNum, midKey);
	// if the splitted non-leaf node is root, generate a new root
	if( curpagenum == rootPageNum)
	{
		Page* newRootPage;
		PageId newRootNum;
		bufMgr -> allocPage(file, newRootNum, newRootPage);
		NonLeafNodeInt* newRootNode = (NonLeafNodeInt*)newRootPage;
		newRootNode -> level = 0;
		insert_nonleaf(*left_pair, *right_pair, newRootNode);
		bufMgr -> unPinPage(file, newRootNum, true);
		changeRootNum(newRootNum);
		//bufMgr -> unPinPage(file, curpagenum, true);
		//bufMgr -> unPinPage(file, newSiblingNum, true);
		bufMgr -> unPinPage(file, newSiblingNum, true);
		return NULL;
	}
	// if the splitted non-leaf node is not root, return the mid pair to the upper level
	else
	{
		//bufMgr -> unPinPage(file, curpagenum, true);
		//bufMgr -> unPinPage(file, newSiblingNum, true);
		bufMgr -> unPinPage(file, newSiblingNum, true);
		return right_pair;
	}
}

const void BTreeIndex::changeRootNum(PageId newRootNum)
{
	rootPageNum = newRootNum;
	Page* headerPage;
	// std::cout << "decleared newRootNum and headerPage" << std::endl;
	bufMgr -> readPage(file, headerPageNum, headerPage);
	// std::cout << "read header page" << std::endl;
	IndexMetaInfo* headerNode = (IndexMetaInfo*)headerPage;
	headerNode -> rootPageNo = newRootNum;
	bufMgr -> unPinPage(file, headerPageNum, true);
}



// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
