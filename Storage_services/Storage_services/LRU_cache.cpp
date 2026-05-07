#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <unistd.h>
#include <fstream> 
#include "storage_type_file.h"
#include "offload_data_to_block.cpp"
using namespace std;





class LRU
{
    public:
        class Node
        {
            public:
                DataBlock data;
                uint64_t key;
                Node *next, *prev;

                Node(uint64_t k, DataBlock d) : key(k), data(d), next(nullptr), prev(nullptr) {} 
        };

        unordered_map<uint64_t, Node*> mp;
        Node *head, *tail;
        int cap;
        BlockStorage *storage;

        LRU(int capacity, BlockStorage *store)
        {
            cap = capacity;
            storage = store;
            head = new Node(-1, {});
            tail = new Node(-1, {});
            head->next = tail;
            tail->prev = head;
        }

        void addNode(Node *node)
        {
            Node* temp = head->next;
            node->prev = head;
            node->next = temp;
            head->next = node;
            temp->prev = node;
        }
        
        void  deleteNode(Node *node)
        {
            Node *nextt = node->next;
            Node *prevv = node->prev;
            prevv->next = nextt;
            nextt->prev = prevv;
        }

        DataBlock get(uint64_t key)
        {
            if(mp.find(key) != mp.end())
            {
                Node *temp = mp[key];
                deleteNode(temp);
                addNode(temp);
                DataBlock d = temp->data;
                return d;
            }
            return {};
        }

        void put(uint64_t key, DataBlock d)
        {
            if(mp.find(key) != mp.end())
            {
                Node* temp = mp[key];
                deleteNode(temp);
                mp.erase(key);
            }
            if(mp.size() == cap)
            {
                Node* temp = tail->prev;
                deleteNode(temp);
                mp.erase(temp->key);

                storage->write(temp->key, temp->data);
            }
            Node *node = new Node(key, d);
            mp[key] = node;
            addNode(node);
        }
};