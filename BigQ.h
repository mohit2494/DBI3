#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include<unordered_map>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include "Comparison.h"
using namespace std;


// ------------------------------------------------------------------
// structure to encapsulate Data for Runmanager
typedef struct{
    int startPage;
    int currentPage;
    int endPage;
    int runId;
} RunFileObject;

// structure to encapsulate Data for Priority Queue
typedef struct{
    int runId;
    Record * record;
} QueueObject;

// structure to encapsulate Data Passed to BigQ's Constructor
typedef struct {
    Pipe * in;
    Pipe * out;
    OrderMaker * sortorder;
    int runlen;
} ThreadData;
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class run {
    int runLength;
    OrderMaker * sortorder;
    vector <Page *> pages;
    public:
        run(int runLength);
        run(int runLength,OrderMaker * sortorder);
        void AddPage();
//      function to add page
        void AddPage(Page *p);
//      function to add record to the page.
        int addRecordAtPage(long long int pageCount, Record *rec);
//      function to check if the run if full.
        bool checkRunFull();
//      function to empty the if the run if full.
        bool clearPages();
//      function to get runSize.
        int getRunSize();
        vector<Page*> getPages();
//      function to getPages For Priority Queue.
        void getPages(vector<Page*> * pagevector);
        bool customRecordComparator(Record &left, Record &right);
//      function to writeRun to File after Sorting.
        int writeRunToFile(File *file);
};
// ------------------------------------------------------------------


// ------------------------------------------------------------------
// Class to implement custom comparator for vector sorting and priority queue sorting.
class CustomComparator{
    ComparisonEngine myComparisonEngine;
    OrderMaker * myOrderMaker;
public:
    CustomComparator(OrderMaker * sortorder);
//  Custom Funtion for sorting vector of records
    bool operator()( Record* lhs,   Record* rhs);
//  Custom Funtion for sorting in priority queue.
    bool operator()(QueueObject lhs,  QueueObject rhs);

};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// Class is used to fetch
class RunManager{
    int noOfRuns;
    int runLength;
    int totalPages;
    File file;
    char * f_path;
    unordered_map<int,RunFileObject> runLocation;
public:
    RunManager(int runLength,char * f_path);
//  Function to get Inital Set of Pages
    void getPages(vector<Page*> * myPageVector);
//  Function to get Next Page for a particular Run
    bool getNextPageOfRun(Page * page,int runNo);

    ~RunManager();
    int getNoOfRuns();
    int getRunLength();
    int getTotalPages();


};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// Class used to sort the records within a run or across runs using priority queue.
class TournamentTree{
    OrderMaker * myOrderMaker;
    RunManager * myRunManager;
    vector<Page*> myPageVector;
    Page OutputBuffer;
    bool isRunManagerAvailable;
    priority_queue<QueueObject,vector<QueueObject>,CustomComparator> * myQueue;
//  Function to initiate and process the queue with records pulled from runManager
    void Inititate();
//  Function to initiate and process the queue with records pulled from run
    void InititateForRun();
public:
    TournamentTree(run * run,OrderMaker * sortorder);
    TournamentTree(RunManager * manager,OrderMaker * sortorder);
//  Function to refill output buffer using RunManger.
    void RefillOutputBuffer();
//  Function to get sorted output buffer and refill buffer again
    bool GetSortedPage(Page * *p);
//  Function to refill output buffer using Run.
    void RefillOutputBufferForRun();
//  Function to get sorted output buffer and refill buffer again
    bool GetSortedPageForRun(Page * *p);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// Class used to sort the heap binary files.
class BigQ {
     ThreadData myThreadData;
     TournamentTree * myTree;
     pthread_t myThread;
     int totalRuns;
     File myFile;
     char * f_path;
//   function to implement phase1 of TPMMS algorithm
     void Phase1();
//   function to implement phase2 of TPMMS algorithm
     void Phase2();

public:
    void sortCompleteRun(run *run, OrderMaker *sortorder);
    //   public static function to drive the TPMMS algorithm.
     static void* Driver(void*);
    //   constructor
     BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    //   destructor
     ~BigQ ();
};
// ------------------------------------------------------------------
#endif
