#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <unistd.h>
#include <fstream> 
#include "disk_location.h"
#include "storage_type_file.h"
using namespace std;

/*
struct DiskLocation
{
    uint64_t offset;
    uint32_t size;
};
*/

/*
  struct DataBlock {
      uint64_t block_id;
      uint32_t size;    
      uint8_t* data;
      uint64_t offset;
  }; 
  */
class BlockStorage
{
    public:
        unordered_map<uint64_t, DiskLocation> index;
        string path;

        BlockStorage(string storage_path) : path(storage_path) {};
        
        void write(uint64_t key, DataBlock block)
        {
            fstream file(path, ios::binary | ios::in | ios::out);
            if(!file.is_open()) throw runtime_error("Cannot open storage : " + path);

            file.seekp(block.offset);
            file.write(reinterpret_cast<char*>(block.data), block.size);

            index[key] = {block.offset, block.size};
            delete[] block.data;   // frees the heap memory — RAM is released
            block.data = nullptr;  // sets the pointer to null so nobody accidentally uses it

        }

        DataBlock read(uint64_t key)
        {
            auto it = index.find(key);
            if(it == index.end()) return {};

            DataBlock d;

            d.block_id = key;
            d.size = it->second.size;
            d.offset = it->second.offset;
            d.data = new uint8_t[d.size];

            ifstream file(path, ios::binary);
            file.seekg(d.offset);
            file.read(reinterpret_cast<char*>(d.data), d.size);

            index.erase(key);

            return d;
        }

        bool has(uint64_t key)
        {
            return index.count(key) > 0;
        }
};