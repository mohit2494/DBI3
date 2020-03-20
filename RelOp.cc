#include "RelOp.h"

//------------------------------------------------------------------------------------------------
/*
SelectFile takes a DBFile and a pipe as input. You can assume that this file is all set up; 
It has been opened and is ready to go. It also takes a CNF. It then performs a scan of the 
underlying file, and for every tuple accepted by the CNF, it stuffs the tuple into the pipe
as output. The DBFile should not be closed by the SelectFile class;that is the job of the caller.
*/
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	// initialize values of class and create thread
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	// create thread and pass this object for use by the thread
	pthread_create(&this->thread, NULL, caller, (void*)this);
}

void SelectFile::WaitUntilDone () {
	// function shall suspend execution of the calling thread until the target thread 
	// terminates, unless the target thread has already terminated
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	// not using bigq
	return;
}

void* SelectFile::caller(void *args) {
	((SelectFile*)args)->operation();
}

// function is called by the thread
void* SelectFile::operation() {
	// given that the file is open, move to first record
	inFile->MoveFirst();
	Record rec;
	ComparisonEngine cmp;
	while(inFile->GetNext(rec)) {
		if (cmp.Compare(&rec, literal, selOp)) {
			outPipe->Insert(&rec);
		}
	}
	outPipe->ShutDown();
}

//------------------------------------------------------------------------------------------------
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}
void SelectPipe::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void SelectPipe::Use_n_Pages (int n) { 
	return;
}

void* SelectPipe::caller(void *args) {
	((SelectPipe*)args)->operation();
}

// function is called by the thread
void* SelectPipe::operation() {
	// given that the file is open, move to first record
	Record rec;
	ComparisonEngine cmp;
	while(inPipe->Remove(&rec)) {
		if (cmp.Compare(&rec, literal, selOp)) {
			outPipe->Insert(&rec);
		}
	}
	outPipe->ShutDown();
}

//------------------------------------------------------------------------------------------------
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

void Project::WaitUntilDone () { 
	pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int n) { 
	return;
}

void* Project::caller(void *args) {
	((Project*)args)->operation();
}

// function is called by the thread
void* Project::operation() {
	Record rec;
	int count = 0;
	while (inPipe->Remove(&rec)) {
		count ++;
		rec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&rec);
	}
	outPipe->ShutDown();
}

//------------------------------------------------------------------------------------------------
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 

}
void Join::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void Join::Use_n_Pages (int n) {

}


//------------------------------------------------------------------------------------------------
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 

}
void DuplicateRemoval::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void DuplicateRemoval::Use_n_Pages (int n) { 
	
}


//------------------------------------------------------------------------------------------------
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

}
void Sum::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void Sum::Use_n_Pages (int n) { 

}

//------------------------------------------------------------------------------------------------
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 

}
void GroupBy::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void GroupBy::Use_n_Pages (int n) { 

}


//------------------------------------------------------------------------------------------------
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 

}
void WriteOut::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void WriteOut::Use_n_Pages (int n) { 

}
