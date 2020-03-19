#include "BigQ.h"
#include "Utilities.h"
using namespace std;

// ------------------------------------------------------------------
void* BigQ :: Driver(void *p){
  BigQ * ptr = (BigQ*) p;
  ptr->Phase1();
  ptr->Phase2();
  ptr->myThreadData.out->ShutDown();
}
void BigQ :: Phase1()
{
    Record tRec;
    run tRun(this->myThreadData.runlen,this->myThreadData.sortorder);

    // add 1 page for adding records
    long long int pageCount=0;
    long long int runCount=1;
    tRun.AddPage();
    bool diff = false;

    // read data from in pipe sort them into runlen pages

    while(this->myThreadData.in->Remove(&tRec)) {
        if(!tRun.addRecordAtPage(pageCount, &tRec)) {
            if (tRun.checkRunFull()) {
                sortCompleteRun(&tRun, this->myThreadData.sortorder);
                diff = tRun.writeRunToFile(&this->myFile);
                if (diff){
                    pageCount= 0;
                    runCount++;
                    tRun.addRecordAtPage(pageCount, &tRec);
                }
                else{
                    tRun.clearPages();
                    tRun.AddPage();
                    pageCount = 0;
                    tRun.addRecordAtPage(pageCount, &tRec);
                }
            }
            else{
                tRun.AddPage();
                pageCount++;
                tRun.addRecordAtPage(pageCount, &tRec);
            }
        }
    }
    if(tRun.getRunSize()!=0) {
        sortCompleteRun(&tRun, this->myThreadData.sortorder);
        tRun.writeRunToFile(&this->myFile);
        tRun.clearPages();
    }
    this->f_path = "temp.xbin";
    this->totalRuns = runCount;
}

// sort runs from file using Run Manager
void BigQ :: Phase2()
{
    RunManager runManager(this->myThreadData.runlen,this->f_path);
    myTree = new TournamentTree(&runManager,this->myThreadData.sortorder);
    Page * tempPage;
    while(myTree->GetSortedPage(&tempPage)){
        Record tempRecord;
        while(tempPage->GetFirst(&tempRecord)){
            this->myThreadData.out->Insert(&tempRecord);
        }
        myTree->RefillOutputBuffer();
    }
}

void BigQ::sortCompleteRun(run *run, OrderMaker *sortorder) {
    myTree = new TournamentTree(run,sortorder);
    Page * tempPage;
    // as run was swapped by tournament tree
    // we need to allocate space for pages again
    // these pages will be part of complete sorted run
    // add 1 page for adding records
    while(myTree->GetSortedPageForRun(&tempPage)){
        Record tempRecord;
        Page * pushPage = new Page();
        while(tempPage->GetFirst(&tempRecord)){
            pushPage->Append(&tempRecord);
        }
        run->AddPage(pushPage);
        myTree->RefillOutputBufferForRun();
    }
    delete myTree;myTree=NULL;
}

// constructor
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    myThreadData.in = &in;
    myThreadData.out = &out;
    myThreadData.sortorder = &sortorder;
    myThreadData.runlen = runlen;
    myTree=NULL;f_path=NULL;
    pthread_create(&myThread, NULL, BigQ::Driver,this);
    //pthread_join(myThread, NULL);
    //out.ShutDown ();
}

// destructor
BigQ::~BigQ () {
    if(Utilities::checkfileExist("temp.xbin")) {
        if( remove( "temp.xbin" ) != 0 )
        cerr<< "Error deleting file" ;
    }
}
// ------------------------------------------------------------------

// ------------------------------------------------------------------
run::run(int runlen) {
    this->runLength = runlen;
    this->sortorder = NULL;
}
run::run(int runlen,OrderMaker * sortorder) {
    this->runLength = runlen;
    this->sortorder = sortorder;
}
void run::AddPage() {
    this->pages.push_back(new Page());
}
void run::AddPage(Page *p) {
    this->pages.push_back(p);
}
vector<Page*> run::getPages() {
    return this->pages;
}
void run::getPages(vector<Page*> * myPageVector) {
    myPageVector->swap(this->pages);
}
bool run::checkRunFull() {
    return this->pages.size() == this->runLength;
}
bool run::clearPages() {
    this->pages.clear();
}
int run::getRunSize() {
    return this->pages.size();
}
int run::addRecordAtPage(long long int pageCount, Record *rec) {
    return this->pages.at(pageCount)->Append(rec);
}
int run::writeRunToFile(File *file) {
    int writeLocation=0;
    if(!Utilities::checkfileExist("temp.xbin")) {
        file->Open(0,"temp.xbin");
    }
    else{
        file->Open(1,"temp.xbin");
        writeLocation=file->GetLength()-1;
    }
    int loopend = pages.size()>runLength ? runLength:pages.size();
    bool difference = false;
    for(int i=0;i<loopend;i++) {
        Record tempRecord;
        Page tempPage;
        while(pages.at(i)->GetFirst(&tempRecord)) {
            tempPage.Append(&tempRecord);
        }
        //write this page to file
        file->AddPage(&tempPage,writeLocation);writeLocation++;
    }
    if(pages.size()>runLength){
        Page *lastPage = new Page();
        Record temp;
        while(pages.back()->GetFirst(&temp)) {
            lastPage->Append(&temp);
        }
        this->clearPages();
        this->AddPage(lastPage);
        difference=true;
    }
    file->Close();
    return difference;
}
// ------------------------------------------------------------------

// ------------------------------------------------------------------
TournamentTree :: TournamentTree(run * run,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    myQueue = new priority_queue<QueueObject,vector<QueueObject>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = false;
    run->getPages(&myPageVector);
    InititateForRun();
}

void TournamentTree :: InititateForRun(){
    if (!myPageVector.empty()){
        int runId = 0;
        for(vector<Page*>::iterator i = myPageVector.begin() ; i!=myPageVector.end() ; ++i){
            Page * page = *(i);
            while(1)
            {
                QueueObject object;
                object.record = new Record();
                object.runId = runId;
                if(!page->GetFirst(object.record)){
                    break;
                }
                myQueue->push(object);
            }
            runId++;
        }

        while(!myQueue->empty()){
            QueueObject topObject = myQueue->top();
            bool OutputBufferFull = !(OutputBuffer.Append(topObject.record));
            if (OutputBufferFull){
                break;
            }
            myQueue->pop();
        }

    }
}

bool TournamentTree :: GetSortedPageForRun(Page ** page){
    if (OutputBuffer.getNumRecs()>0){
        *(page) = &OutputBuffer;
        return 1;
    }
    return 0;
}

void TournamentTree :: RefillOutputBufferForRun(){
     if (!myPageVector.empty()){
         while(!myQueue->empty()){
             QueueObject topObject = myQueue->top();
             bool OutputBufferFull = !(OutputBuffer.Append(topObject.record));
             if (OutputBufferFull){
                 break;
             }
             myQueue->pop();
         }
     }
}


TournamentTree :: TournamentTree(RunManager * manager,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    myRunManager = manager;
    myQueue = new priority_queue<QueueObject,vector<QueueObject>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = true;
    myRunManager->getPages(&myPageVector);
    Inititate();
}
void TournamentTree :: Inititate(){
    if (!myPageVector.empty()){
        int runId = 0;
        for(vector<Page*>::iterator i = myPageVector.begin() ; i!=myPageVector.end() ; ++i){
            Page * page = *(i);
            QueueObject object;
            object.record = new Record();
            object.runId = runId++;
            if(page->GetFirst(object.record)){
                myQueue->push(object);
            }
        }

        while(!myQueue->empty()){
            QueueObject topObject = myQueue->top();
            if(isRunManagerAvailable){
            }

            bool OutputBufferFull = !(OutputBuffer.Append(topObject.record));
            if (OutputBufferFull){
                break;
            }
            myQueue->pop();
            Page * topPage = myPageVector.at(topObject.runId);
            if (!(topPage->GetFirst(topObject.record))){
                if (isRunManagerAvailable&&myRunManager->getNextPageOfRun(topPage,topObject.runId)){
                    if(topPage->GetFirst(topObject.record)){
                        myQueue->push(topObject);
                    }
                }
            }
            else{
                myQueue->push(topObject);
            }
        }

    }
}

void TournamentTree :: RefillOutputBuffer(){
    if (!myPageVector.empty()){
        int runId = 0;
        while(!myQueue->empty()){
            QueueObject topObject = myQueue->top();
            bool OutputBufferFull = !(OutputBuffer.Append(topObject.record));
            if (OutputBufferFull){
                break;
            }
            myQueue->pop();

            Page * topPage = myPageVector.at(topObject.runId);
            if (!(topPage->GetFirst(topObject.record))){
                if (isRunManagerAvailable&&myRunManager->getNextPageOfRun(topPage,topObject.runId)){
                    if(topPage->GetFirst(topObject.record)){
                        myQueue->push(topObject);
                    }
                }
            }
            else{
                myQueue->push(topObject);
            }

        }
    }
}



bool TournamentTree :: GetSortedPage(Page ** page){
    if (OutputBuffer.getNumRecs()>0){
        *(page) = &OutputBuffer;
        return 1;
    }
    return 0;
}
// ------------------------------------------------------------------


// ------------------------------------------------------------------
RunManager :: RunManager(int runLength,char * f_path){
    this->runLength = runLength;
    this->f_path = f_path;
    this->file.Open(1,this->f_path);
    int totalPages = file.GetLength()-1;
    this->noOfRuns = totalPages/runLength + (totalPages % runLength != 0);
    this->totalPages = totalPages;
    int pageOffset = 0;
    for(int i = 0; i<noOfRuns;i++){
        RunFileObject fileObject;
        fileObject.runId = i;
        fileObject.startPage = pageOffset;
        fileObject.currentPage = fileObject.startPage;
        pageOffset+=runLength;
        if (pageOffset <=totalPages){
            fileObject.endPage = pageOffset-1;
        }
        else{
            fileObject.endPage =  fileObject.startPage + (totalPages-fileObject.startPage-1);
        }
        runLocation.insert(make_pair(i,fileObject));
    }

}


void  RunManager:: getPages(vector<Page*> * myPageVector){
    for(int i = 0; i< noOfRuns ; i++){
        unordered_map<int,RunFileObject>::iterator runGetter = runLocation.find(i);
        if(!(runGetter == runLocation.end())){
            Page * pagePtr = new Page();
            this->file.GetPage(pagePtr,runGetter->second.currentPage);
            runGetter->second.currentPage+=1;
            if(runGetter->second.currentPage>runGetter->second.endPage){
                runLocation.erase(i);
            }
            myPageVector->push_back(pagePtr);
        }
    }
}

bool RunManager :: getNextPageOfRun(Page * page,int runNo){
    unordered_map<int,RunFileObject>::iterator runGetter = runLocation.find(runNo);
    if(!(runGetter == runLocation.end())){
        this->file.GetPage(page,runGetter->second.currentPage);
        runGetter->second.currentPage+=1;
        if(runGetter->second.currentPage>runGetter->second.endPage){
            runLocation.erase(runNo);
        }
        return true;
    }
    return false;
}
RunManager :: ~RunManager(){
    file.Close();
}

int RunManager :: getNoOfRuns()
{
  return noOfRuns;
}


int RunManager ::getRunLength()
{
  return runLength;
}

int RunManager :: getTotalPages()
{
  return totalPages;
}

// ------------------------------------------------------------------


// ------------------------------------------------------------------
CustomComparator :: CustomComparator(OrderMaker * sortorder){
    this->myOrderMaker = sortorder;
}
bool CustomComparator :: operator ()( QueueObject lhs, QueueObject rhs){
    int val = myComparisonEngine.Compare(lhs.record,rhs.record,myOrderMaker);
    return (val <=0)? false : true;
}

bool CustomComparator :: operator ()( Record* lhs, Record* rhs){
    int val = myComparisonEngine.Compare(lhs,rhs,myOrderMaker);
    return (val <0)? true : false;
}
// ------------------------------------------------------------------
