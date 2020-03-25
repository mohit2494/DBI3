#include "DBFile.h"
#include "Schema.h"
#include "Utilities.h"
#include "RelOp.h"
#include <string.h>
#include <gtest/gtest.h>
#include "test.h"

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		int ival = 0; double dval = 0;
		func.Apply (rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}

int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz);
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
Function func_ps, func_p, func_s, func_o, func_li, func_c;

int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;

void init_SF_ps (char *pred_str, int numpgs) {
	dbf_ps.Open (ps->path());
	get_cnf (pred_str, ps->schema (), cnf_ps, lit_ps);
	SF_ps.Use_n_Pages (numpgs);
}

void init_SF_p (char *pred_str, int numpgs) {
	dbf_p.Open (p->path());
	get_cnf (pred_str, p->schema (), cnf_p, lit_p);
	SF_p.Use_n_Pages (numpgs);
}

void init_SF_s (char *pred_str, int numpgs) {
	dbf_s.Open (s->path());
	get_cnf (pred_str, s->schema (), cnf_s, lit_s);
	SF_s.Use_n_Pages (numpgs);
}

void init_SF_o (char *pred_str, int numpgs) {
	dbf_o.Open (o->path());
	get_cnf (pred_str, o->schema (), cnf_o, lit_o);
	SF_o.Use_n_Pages (numpgs);
}

void init_SF_li (char *pred_str, int numpgs) {
	dbf_li.Open (li->path());
	get_cnf (pred_str, li->schema (), cnf_li, lit_li);
	SF_li.Use_n_Pages (numpgs);
}

void init_SF_c (char *pred_str, int numpgs) {
	dbf_c.Open (c->path());
	get_cnf (pred_str, c->schema (), cnf_c, lit_c);
	SF_c.Use_n_Pages (numpgs);
}


TEST(QueryTesting, GettingUniqueFilePath) {
    
    char *fileName1 = Utilities::newRandomFileName(".bin");
    char *fileName2 = Utilities::newRandomFileName(".bin");
    ASSERT_TRUE(fileName1!=fileName2);
}

TEST(QueryTesting, sum) {
    setup();
    char *pred_s = "(s_suppkey = s_suppkey)";
    init_SF_s (pred_s, 100);

    Sum T;
    // _s (input pipe)
    Pipe _out (1);
    Function func;
    char *str_sum = "(s_acctbal)/10000000";
    get_cnf (str_sum, s->schema (), func);
    func.Print ();
    T.Use_n_Pages (1);
    SF_s.Run (dbf_s, _s, cnf_s, lit_s);
    T.Run (_s, _out, func);

    SF_s.WaitUntilDone ();
    T.WaitUntilDone ();

    Schema out_sch ("out_sch", 1, &DA);
    
    Record t;
    _out.Remove(&t);
    int pointer = ((int *) t.bits)[1];
    double *md = (double *) &(t.bits[pointer]);
    double test = *md;
    dbf_s.Close ();
    EXPECT_NEAR(4.51035,test, 1);
}

TEST(QueryTesting, WriteOutTesting) {

    DBFile s;
    s.Open("dbfiles/supplier.bin");
    s.MoveFirst();
    char *fwpath = "wo.txt";
	FILE *writefile = fopen (fwpath, "w");
    WriteOut w;
    Pipe ip(PAGE_SIZE);
    Record temp;
    Schema supp("catalog","supplier");
    w.Run(ip, writefile, supp);
    int c = 0;
    while(s.GetNext(temp)) {
        ++c;
        ip.Insert(&temp);
    }
    s.Close();
    ip.ShutDown();
    w.WaitUntilDone();

    int numLines = 0;
    ifstream in("wo.txt");
    std::string unused;
    while ( std::getline(in, unused) )
        ++numLines;
    in.close();

    if(Utilities::checkfileExist("wo.txt")) {
        if( remove("wo.txt") != 0 )
        cerr<< "Error deleting file" ;
    }
    ASSERT_EQ(c, numLines);
}

TEST(QueryTesting, DuplicateRemovalTesting) {
    DBFile s;
    s.Open("dbfiles/supplier.bin");
    s.MoveFirst();
   
    DuplicateRemoval w;
    Pipe ip(PAGE_SIZE);
    Pipe op(PAGE_SIZE);
    Record temp;
    Schema supp("catalog","supplier");
    w.Run(ip,op,supp);
    int c = 0;
    while(s.GetNext(temp)) {
        ++c;
        ip.Insert(&temp);
    }
    s.Close();
    ip.ShutDown();
    w.WaitUntilDone();

    ASSERT_EQ(732,clear_pipe(op, &supp, false));
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}