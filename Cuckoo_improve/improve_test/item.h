#ifndef RESEARCH_ITEM_H
#define RESEARCH_ITEM_H

#define ITEM_KEY(item) (item->buf)
#define ITEM_VALUE(item) (item->buf + key_len)

struct Item{
    size_t key_len;
    size_t value_len;
    char buf[];
};

Item * allocate_item(char * key,size_t key_len,char * value,size_t value_len){
    Item * p = (Item * )malloc(key_len + value_len);
    p->key_len = key_len;
    p->value_len = value_len;
    memcpy(ITEM_KEY(p),key,key_len);
    memcpy(ITEM_VALUE(p),value,value_len);
}

#endif //RESEARCH_ITEM_H
