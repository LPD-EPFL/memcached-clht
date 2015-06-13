#ifndef _CLHT_UTIL_H_
#define _CLHT_UTIL_H_

/* Key comparison of memcached items for clht hashtable
 *
 * key         key to compare
 * nkey        key length
 * item_ptr    stored item to compare key with
 */
int keycmp_key_item(const char* key, const size_t nkey, clht_val_t item_ptr);
int keycmp_item_item(clht_val_t item_ptr1, clht_val_t item_ptr2);

#endif
