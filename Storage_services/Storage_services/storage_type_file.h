  #pragma once                                                                               
  #include <cstdint>                                                                         
                    
  struct DataBlock {
      uint64_t block_id;
      uint32_t size;    
      uint8_t* data;
      uint64_t offset;
  }; 