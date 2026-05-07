

#include <iostream>
#include<stdio.h>
#include <string>
#include <ctime>
#include <chrono>

int main() {
    std::time_t now = std::time(nullptr);   // current system time
    std::time_t other = now + 60;           // example: 60 seconds later


    std::cout<<"now ="<<now<<" other="<<other<<std::endl;
    if (now < other) {
        std::cout << "now is earlier\n";
        
    } else if (now > other) {
        std::cout << "now is later\n";
    } else {
        std::cout << "same time\n";
    }

    std::cout << "Difference in seconds: " << std::difftime(other, now) << '\n';
}