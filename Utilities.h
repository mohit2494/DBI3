#include "string"
#include "iostream"
#include "fstream"

// #include "cstdio"
// #include <ctime>
/**
 *  class for defining utility functions to be used
 *  across the project. Import "utility.h" to use it.
**/
class Utilities {
    
    public:
    
        // check if file exists already at the given file path
        static int checkfileExist (const std::string& name) {
            if (FILE *file = fopen(name.c_str(), "r")) {
                fclose(file);
                return 1;
            } else {
                return 0;
            }   
        }
        static int getNextCounter () {
            ifstream ifile;
            ofstream ofile;
            int counter = 0;
            if (checkfileExist("counter.txt")){
                ifile.open("counter.txt",ios::in);
                ifile.read((char*)&counter,sizeof(int));
                ifile.close();
            }
            counter++;
            ofile.open("counter.txt",ios::out);
            ofile.write((char*)&counter,sizeof(int));
            ofile.close();
            return counter;
        }

        // static int Log(string s) {
        //      if(std::freopen("runtime.log", "a+", stdout)) {
        //         s = "\n"+Utilities::getCurrentTime() + " : " + s +"\n";
        //         std::printf(s.c_str()); // this is written to redir.txt
        //         std::fclose(stdout);
        //     }
        // }

        // static std::string getCurrentTime() {
        //     time_t rawtime;
        //     struct tm * timeinfo;
        //     char buffer[80];

        //     time (&rawtime);
        //     timeinfo = localtime(&rawtime);

        //     strftime(buffer,sizeof(buffer),"%d-%m-%Y %H:%M:%S",timeinfo);
        //     std::string str(buffer);

        //     return str;
        // }

};
