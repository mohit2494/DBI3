#include "DBFile.h"
#include "Schema.h"
#include <string.h>
#include <gtest/gtest.h>


class FilePath{
    public:
    const std::string dbfile_dir = "dbfiles/"; 
    const std::string tpch_dir ="/home/mk/Documents/uf docs/sem 2/Database Implementation/git/tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
};


TEST(QueryTesting, SelectFile) {
    
    // creating DBFile for nation table
    // ASSERT_EQ(0,dbfile.Create(dbFilePath.c_str(),heap,NULL));
}

TEST(QueryTesting, SelectPipe) {

    
    
    
    // Trying to open a file which doesn't exist
    // ASSERT_EQ(0,dbfile.Open(nonExistentPath.c_str()));

}

TEST(QueryTesting, Project) {

    
    // ASSERT_EQ(1,dbfile.Open(existentPath.c_str()));

}

TEST(QueryTesting, Sum) {

   
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}