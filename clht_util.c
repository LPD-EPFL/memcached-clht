#include <string.h>
#include "clht.h"
#include "clht_util.h"
#include "memcached.h"

int keycmp_key_item(const char* key, const size_t nkey, clht_val_t item_ptr)
{
    // TODO: implement fast key compare function
    item* it = (item*)item_ptr;
    return ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0));
}

int keycmp_item_item(clht_val_t item_ptr1, clht_val_t item_ptr2)
{
    // TODO: implement fast key compare function
    item* it1 = (item*)item_ptr1;
    item* it2 = (item*)item_ptr2;
    return ((it1->nkey == it2->nkey) && (memcmp(ITEM_key(it1), ITEM_key(it2), it1->nkey) == 0));
}

