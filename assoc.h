/* associative array */
void assoc_init(const int hashpower_init);
#ifdef CLHT
void assoc_thread_init(int thread_id);
#endif
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv);
int assoc_insert(item *item, const uint32_t hv);
#ifdef CLHT
item* assoc_replace(item* it, const uint32_t hv);
#endif
int assoc_delete(const char *key, const size_t nkey, const uint32_t hv);
void do_assoc_move_next_bucket(void);
int start_assoc_maintenance_thread(void);
void stop_assoc_maintenance_thread(void);
extern unsigned int hashpower;
extern unsigned int item_lock_hashpower;
