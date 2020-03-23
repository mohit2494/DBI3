#include "RelOp.h"
#include<sstream>

#define CLEANUPVECTOR(v) \
	({ for(vector<Record *>::iterator it = v.begin(); it!=v.end(); it++) { \
		if(!*it) { delete *it; } }\
		v.clear();\
})

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
			++count;
		}
	}
	cout << "SF :" << count << endl;
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
	while (inPipe->Remove(&rec)) { 
		rec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&rec);
	}
	outPipe->ShutDown();
}
//------------------------------------------------------------------------------------------------
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, caller, (void *)this);

}
void Join::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void Join::Use_n_Pages (int n) {
	rl = n;
}
void* Join::caller(void *args) {
	((Join*)args)->operation();
}
// function is called by the thread
void* Join::operation() {
	
	Pipe spl(500), spr(500);
	OrderMaker lom,rom;ComparisonEngine ce;
	selOp->GetSortOrders(lom,rom);
	BigQ lq(*inPipeL, spl, lom, rl); 
	BigQ rq(*inPipeR, spr, rom, rl);
	Record lr, rr, m;
	
	
	// normal join
	if(lom.getNumAtts()==rom.getNumAtts()!=0) {
		bool le=spl.Remove(&lr); bool re=spr.Remove(&rr);
		int c=0;int lc=1; int rc=1;
		
		while(le&&re) {
			int v = ce.Compare(&lr,&lom, &rr, &rom);
			if (v==-1)    { le=spl.Remove(&lr);lc++;}
			else if(v==1) { re=spr.Remove(&rr);rc++;}
			else {
				vector<Record *> vl; vector <Record *> vr;
				Record *pushlr  = new Record();  pushlr->Consume(&lr);  vl.push_back(pushlr);
				Record *pushrr  = new Record();  pushrr->Consume(&rr);  vr.push_back(pushrr);
				
				le=spl.Remove(&lr);lc++;
				while(le && (!ce.Compare(&lr,pushlr,&lom))) {
					Record *tr = new Record(); tr->Consume(&lr); vl.push_back(tr);le=spl.Remove(&lr);lc++;
				}

				re=spr.Remove(&rr);rc++;
				while(re &&(!ce.Compare(&rr,pushrr,&rom))) {
					Record *tr = new Record(); tr->Consume(&rr); vr.push_back(tr);re=spr.Remove(&rr);rc++;
				}
				c++;
				for(int i=0; i < vl.size(); i++){ 
					for(int j=0; j< vr.size(); j++) {
						MergeRecord(vl[i], vr[j]);
					}
				}
				CLEANUPVECTOR(vl);CLEANUPVECTOR(vr);
			}
		}
		
	}
	// block-nested join
	else {}
	while(spl.Remove(&lr)) {}
	while(spr.Remove(&rr)) {}

	outPipe->ShutDown();
}

void Join::MergeRecord(Record *lr, Record *rr) {
	int nal=lr->getNumAtts(), nar=rr->getNumAtts();
	int *atts = new int[nal+nar];
	for (int k=0;k<nal;k++) atts[k]=k;
	for (int k=0;k<nar;k++) atts[k+nal]=k;
	Record *m = new Record();
	m->MergeRecords(lr, rr, nal, nar, atts, nal+nar, nal);
	outPipe->Insert(m);
	delete atts;
	return;
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
	this->runlength = n;
}
void* DuplicateRemoval::caller(void *args) {
	((DuplicateRemoval*)args)->operation();
}
// function is called by the thread
void* DuplicateRemoval::operation() {
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
	}
	outPipe->Insert(&pr);outPipe->ShutDown();
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
	rl=n;
}
void* GroupBy::caller(void *args) {
	((GroupBy*)args)->operation();
}
// function is called by the thread
void* GroupBy::operation() {
	
	Pipe sp(1000);BigQ bq(*inPipe,sp,*groupAtts,rl);
    Record *gr = new Record(),*pr=new Record(),*cr=NULL;
	int ir,si;double dr,sd;Type rt;
    ComparisonEngine ce;bool grchng=false,pe = false;
    
	// defensive check
    if(!sp.Remove(pr)) { outPipe->ShutDown(); return 0; }
 
    while(!pe) {    
        si =0;sd=0;ir=0;dr=0;
        cr = new Record();
        grchng = false;
    
        while(!pe && !grchng) {
            sp.Remove(cr);
			// defensive check in case record is empty
            if(cr->bits!=NULL) { 
				if(ce.Compare(cr,pr,groupAtts)!=0){gr->Copy(pr);grchng=true;} 
			}
            else { pe = true; }
			rt = computeMe->Apply(*pr,ir,dr);
			if(rt==Int) { si += ir; } else if(rt==Double) { sd += dr; }
            pr->Consume(cr);
        }
    
        Record *op = new Record();
        if(rt==Double) {
			Attribute a = {(char*)"sum", Double};Schema ss((char*)"somefile",1,&a);
			char sstr[30];sprintf(sstr, "%f|", sd);
			op->ComposeRecord(&ss,sstr);
        }
        if (rt==Int) {
			Attribute att = {(char*)"sum", Int};Schema ss((char*)"somefile",1,&att);
			char sstr[30];sprintf(sstr, "%d|", si);
			op->ComposeRecord(&ss,sstr);            
        }

        Record rr;
        int nsatt = groupAtts->numAtts+1; int satt[nsatt]; satt[0]=0;
        for(int i=1;i<nsatt;i++) {
            satt[i]=groupAtts->whichAtts[i-1];
        }
        rr.MergeRecords(op,gr,1,nsatt-1,satt,nsatt,1);
        outPipe->Insert(&rr);
    }
    outPipe->ShutDown();
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
	Record rec; int cnt=0;
	int n = mySchema->GetNumAtts();
	Attribute *atts = mySchema->GetAtts();
		
	while(inPipe->Remove(&rec)) {
		cnt++;
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
	fclose(outFile);
	//cerr << " number of records written "<<cnt<<endl;
}

