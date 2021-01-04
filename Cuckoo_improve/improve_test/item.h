#ifndef RESEARCH_ITEM_H
#define RESEARCH_ITEM_H

#include "assert_msg.h"

#define ITEM_KEY(item_ptr) ((Item*)item_ptr)->buf
#define ITEM_KEY_LEN(item_ptr)  ((Item * )item_ptr)->key_len
#define ITEM_VALUE(item_ptr) (((Item * )item_ptr)->buf + key_len)
#define ITEM_VALUE_LEN(item_ptr)  ((Item * )item_ptr)->value_len


struct Item{
    uint32_t key_len;
    uint32_t value_len;
    char buf[];
};

Item * allocate_item(char * key,size_t key_len,char * value,size_t value_len){
    Item * p = (Item * )malloc(key_len + value_len + 2 * sizeof(uint32_t));
    ASSERT(p!= nullptr,"malloc failure");
    p->key_len = key_len;
    p->value_len = value_len;
    memcpy(ITEM_KEY(p),key,key_len);
    memcpy(ITEM_VALUE(p),value,value_len);
    return p;
}

#endif //RESEARCH_ITEM_H
