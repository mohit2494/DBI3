#include "RelOp.h"

//------------------------------------------------------------------------------------------------
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	// initialize values of class and create thread
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&this->thread, NULL, caller, (void*)this);
}
void SelectFile::WaitUntilDone () {
	// function shall suspend execution of the calling 
	// thread until the target thread terminates, unless
	// the target thread has already terminated
	pthread_join (thread, NULL);
}
void SelectFile::Use_n_Pages (int runlen) {
	return;
}
void* SelectFile::caller(void *args) {
	((SelectFile*)args)->operation();
}
// function is called by the thread
void* SelectFile::operation() {
	int count=0;
	// given that the file is open, move to first record
	inFile->MoveFirst();
	Record rec;
	ComparisonEngine cmp;
	while(inFile->GetNext(rec)) {
		if (cmp.Compare(&rec, literal, selOp)) {
			outPipe->Insert(&rec);
			count++;
		}
	}
	outPipe->ShutDown();
	cerr<<"SF: "<<count<<endl;
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
	cerr << "Project :" << count << endl;
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
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	pthread_create(&this->thread,NULL, caller, (void*)this);
}
void DuplicateRemoval::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void DuplicateRemoval::Use_n_Pages (int n) { 
	runlength = n;
}
void* DuplicateRemoval::caller(void *args) {
	((DuplicateRemoval*)args)->operation();
}
// function is called by the thread
void* DuplicateRemoval::operation() {
	int c=0;
	OrderMaker om(mySchema); 
	Pipe *sp = new Pipe(100);
	Record pr, cr;	
	BigQ sq(*inPipe, *sp, om, runlength);
	ComparisonEngine ce;
	sp->Remove(&pr);
	while(sp->Remove(&cr)) {
		if(!ce.Compare(&pr, &cr, &om)) continue;
		outPipe->Insert(&pr);
		pr.Consume(&cr);
		c++;
	}
	outPipe->Insert(&pr);outPipe->ShutDown();c++;
	cerr << "DR "<<c<<endl;
}
//------------------------------------------------------------------------------------------------
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}
void Sum::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void Sum::Use_n_Pages (int n) { 
	return;
}
void* Sum::caller(void *args) {
	((Sum*)args)->operation();
}
// function is called by the thread
void* Sum::operation() {
	Record t; Record rec; Type rt;
	int si = 0; double sd = 0.0f;

	while(inPipe->Remove(&rec)) {
		int ti = 0; double td = 0.0f;
		rt = this->computeMe->Apply(rec, ti, td);
		rt == Int ? si += ti : sd += td;
	}

	char result[30];
	if (rt == Int) sprintf(result, "%d|", si); else sprintf(result, "%f|", sd);
	Type myType = (rt==Int)?Int:Double;
	Attribute sum = {(char *)"sum",myType};
	Schema os("something",1,&sum);
	t.ComposeRecord(&os,result);
	outPipe->Insert(&t);outPipe->ShutDown();
}

//------------------------------------------------------------------------------------------------
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}
void GroupBy::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void GroupBy::Use_n_Pages (int n) { 
	return;
}
void* GroupBy::caller(void *args) {
	((GroupBy*)args)->operation();
}
// function is called by the thread
void* GroupBy::operation() {
	
}
//------------------------------------------------------------------------------------------------
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	this->inPipe = &inPipe;
	this->mySchema = &mySchema;
	this->outFile = outFile;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

void WriteOut::WaitUntilDone () { 
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int n) { 
	return;
}

void* WriteOut::caller(void *args) {
	((WriteOut*)args)->operation();
}

// function is called by the thread
void* WriteOut::operation() {
	
	// reference taken from Record.print()
	Record rec; int c=0;
	int n = mySchema->GetNumAtts();
	Attribute *atts = mySchema->GetAtts();
		
	while(inPipe->Remove(&rec)) {
		c++;
		// loop through all of the attributes
		for (int i = 0; i < n; i++) {

			// use the i^th slot at the head of the record to get the
			// offset to the correct attribute in the record
			int pointer = ((int *) rec.bits)[i + 1];

			// here we determine the type, which given in the schema;
			// depending on the type we then print out the contents

			// first is integer
			if (atts[i].myType == Int) {
				char *myInt = (char *) &(rec.bits[pointer]);
				fprintf(outFile,"%d",myInt);

			// then is a double
			} else if (atts[i].myType == Double) {
				char *myDouble = (char *) &(rec.bits[pointer]);
				fprintf(outFile,"%f",myDouble);	

			// then is a character string
			} else if (atts[i].myType == String) {
				char *myString = (char *) &(rec.bits[pointer]);
				fprintf(outFile,"%s",myString);	
			} 

			// print out a comma as needed to make things pretty
			if (i != n - 1) {
				fprintf(outFile,"%s","|");	
			}
		}
		fprintf(outFile,"%s","\n");
	}
	cerr << "File: " << c << endl;
	fclose(outFile);
}

