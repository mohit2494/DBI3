#include "RelOp.h"

//------------------------------------------------------------------------------------------------
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

}

void SelectFile::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}

//------------------------------------------------------------------------------------------------
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 

}
void SelectPipe::WaitUntilDone () { 

}
void SelectPipe::Use_n_Pages (int n) { 

}

//------------------------------------------------------------------------------------------------
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 

}
void Project::WaitUntilDone () { 

}
void Project::Use_n_Pages (int n) { 

}


//------------------------------------------------------------------------------------------------
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 

}
void Join::WaitUntilDone () { 

}
void Join::Use_n_Pages (int n) {

}


//------------------------------------------------------------------------------------------------
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 

}
void DuplicateRemoval::WaitUntilDone () { 

}
void DuplicateRemoval::Use_n_Pages (int n) { 
	
}


//------------------------------------------------------------------------------------------------
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

}
void Sum::WaitUntilDone () { 

}
void Sum::Use_n_Pages (int n) { 

}

//------------------------------------------------------------------------------------------------
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 

}
void GroupBy::WaitUntilDone () { 

}
void GroupBy::Use_n_Pages (int n) { 

}


//------------------------------------------------------------------------------------------------
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 

}
void WriteOut::WaitUntilDone () { 

}
void WriteOut::Use_n_Pages (int n) { 

}
