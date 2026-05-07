#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <unistd.h>
#include <fstream> 
using namespace std;


class logger
{
    private:
        map<std::time_t, unordered_map<string, int>> mp;  // map with time and string
        ofstream logFile;  // Add this member
        void debug(string val)
        {
            std::cout<<"Debug :"<<val<<std::endl;
            logFile<<"Debug :"<<val<<std::endl;
        }
        void info(string val)
        {
            std::cout<<"info:"<<val<<std::endl;
            logFile<<"info:"<<val<<std::endl;
        }
        void error(string val)
        {   
            std::time_t curr_time = std::time(nullptr);
            if(prev_time != curr_time)
            {
                mp[curr_time][val] = 1;
                prev_time = curr_time;
            }
            else
            {
                mp[prev_time][val]++;
            }

            if(mp[curr_time][val] >= 10)
            {
                cout<<"Error:<"<<mp[curr_time][val]<<">"<<val<<std::endl;
                logFile<<"Error:<"<<mp[curr_time][val]<<">"<<val<<std::endl;
            }
            else
            {
                cout<<"Error:"<<val<<std::endl;
                logFile<<"Error:"<<val<<std::endl;
            }
        }
        void warn(string val)
        {
            std::cout<<"warning: "<<val<<std::endl;
            logFile<<"warning: "<<val<<std::endl;
        }

        public:
            std::time_t prev_time;
            logger()
            {
                prev_time = std::time(nullptr);
                logFile.open("log.txt", ios::app);
                logFile << "New log entry from :" << prev_time << std::endl;
            }
            ~logger()
            {
                if(logFile.is_open()) {
                    logFile.close();
                }
            }
            enum Level {
                DEBUG,
                INFO,
                ERROR,
                WARN
            };
            
            void logger_service(Level x, string val)
            {
                switch(x)
                {
                    case DEBUG: debug(val); break;
                    case INFO: info(val); break;
                    case ERROR: error(val); break;
                    case WARN: warn(val); break;
                }
            }
};

int main()
{
    logger l;
    for(int i = 0; i < 50; i++)
    {
        if(i == 15)
        {
            sleep(1);
            l.logger_service(logger::DEBUG, "just checking");

        }
        if(i % 2 == 0)
        {
            l.logger_service(logger::ERROR, "test");
        }
        else
        {
            string t = "test"+std::to_string(i);
            l.logger_service(logger::ERROR, t);
        }
    }
    return 0;
}