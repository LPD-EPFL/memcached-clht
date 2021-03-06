/*   
 *   File: clht_lb_res.h
 *   Author: Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: lock-based cache-line hash table with resizing.
 *   clht_lb_res.h is part of ASCYLIB
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#ifndef _CLHT_RES_RES_H_
#define _CLHT_RES_RES_H_

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <xmmintrin.h>

#include "atomic_ops.h"
#include "ssmem.h"

extern __thread ssmem_allocator_t* clht_alloc;

#define true 1
#define false 0

/* #define DEBUG */

#define CLHT_READ_ONLY_FAIL   1
#define CLHT_HELP_RESIZE      0
#define CLHT_PERC_EXPANSIONS  1
#define CLHT_MAX_EXPANSIONS   24
#define CLHT_PERC_FULL_DOUBLE 50	   /* % */
#define CLHT_RATIO_DOUBLE     2		  
#define CLHT_OCCUP_AFTER_RES  40
#define CLHT_PERC_FULL_HALVE  5		   /* % */
#define CLHT_RATIO_HALVE      8		  
#define CLHT_MIN_CLHT_SIZE    8
#define CLHT_DO_CHECK_STATUS  0
#define CLHT_DO_GC            0
#define CLHT_STATUS_INVOK     500000
#define CLHT_STATUS_INVOK_IN  500000
#define LOAD_FACTOR           2

#if defined(RTM)	       /* only for processors that have RTM */
#define CLHT_USE_RTM          1
#else
#define CLHT_USE_RTM          0
#endif

#if CLHT_DO_CHECK_STATUS == 1
#  define CLHT_CHECK_STATUS(h)				\
  if (unlikely((--check_ht_status_steps) == 0))		\
    {							\
      ht_status(h, 0, 0);				\
      check_ht_status_steps = CLHT_STATUS_INVOK;	\
    }

#else 
#  define CLHT_CHECK_STATUS(h)
#endif

#if CLHT_DO_GC == 1
#  define CLHT_GC_HT_VERSION_USED(ht) clht_gc_thread_version((clht_hashtable_t*) ht)
#else
#  define CLHT_GC_HT_VERSION_USED(ht)
#endif


/* CLHT LINKED version specific parameters */
#define CLHT_LINKED_PERC_FULL_DOUBLE       75
#define CLHT_LINKED_MAX_AVG_EXPANSION      1
#define CLHT_LINKED_MAX_EXPANSIONS         7
#define CLHT_LINKED_MAX_EXPANSIONS_HARD    16
#define CLHT_LINKED_EMERGENCY_RESIZE       4 /* how many times to increase the size on emergency */
/* *************************************** */

#if defined(DEBUG)
#  define DPP(x)	x++				
#else
#  define DPP(x)
#endif

#define CACHE_LINE_SIZE    64
#define ENTRIES_PER_BUCKET 3

#ifndef ALIGNED
#  if __GNUC__ && !SCC
#    define ALIGNED(N) __attribute__ ((aligned (N)))
#  else
#    define ALIGNED(N)
#  endif
#endif

#define likely(x)       __builtin_expect((x), 1)
#define unlikely(x)     __builtin_expect((x), 0)

#if defined(__sparc__)
#  define PREFETCHW(x) 
#  define PREFETCH(x) 
#  define PREFETCHNTA(x) 
#  define PREFETCHT0(x) 
#  define PREFETCHT1(x) 
#  define PREFETCHT2(x) 

#  define PAUSE    asm volatile("rd    %%ccr, %%g0\n\t" \
				::: "memory")
#  define _mm_pause() PAUSE
#  define _mm_mfence() __asm__ __volatile__("membar #LoadLoad | #LoadStore | #StoreLoad | #StoreStore");
#  define _mm_lfence() __asm__ __volatile__("membar #LoadLoad | #LoadStore");
#  define _mm_sfence() __asm__ __volatile__("membar #StoreLoad | #StoreStore");


#elif defined(__tile__)
#  define _mm_lfence() arch_atomic_read_barrier()
#  define _mm_sfence() arch_atomic_write_barrier()
#  define _mm_mfence() arch_atomic_full_barrier()
#  define _mm_pause() cycle_relax()
#endif

#define CAS_U64_BOOL(a, b, c) (CAS_U64(a, b, c) == b)

typedef uintptr_t clht_addr_t;
typedef volatile uintptr_t clht_val_t;

#if defined(__tile__)
typedef volatile uint32_t clht_lock_t;
#else
typedef volatile uint8_t clht_lock_t;
#endif

typedef struct ALIGNED(CACHE_LINE_SIZE) bucket_s
{
  clht_lock_t lock;
  volatile uint32_t hops;
  clht_addr_t key[ENTRIES_PER_BUCKET];
  clht_val_t val[ENTRIES_PER_BUCKET];
  volatile struct bucket_s* next;
} bucket_t;

#if __GNUC__ > 4 && __GNUC_MINOR__ > 4
_Static_assert (sizeof(bucket_t) % 64 == 0, "sizeof(bucket_t) == 64");
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
typedef struct ALIGNED(CACHE_LINE_SIZE) clht
{
  union
  {
    struct
    {
      struct clht_hashtable_s* ht;
      uint8_t next_cache_line[CACHE_LINE_SIZE - (sizeof(void*))];
      struct clht_hashtable_s* ht_oldest;
      struct ht_ts* version_list;
      size_t version_min;
      volatile clht_lock_t resize_lock;
      volatile clht_lock_t gc_lock;
      volatile clht_lock_t status_lock;
    };
    uint8_t padding[2 * CACHE_LINE_SIZE];
  };
} clht_t;
#pragma GCC diagnostic pop

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
typedef struct ALIGNED(CACHE_LINE_SIZE) clht_hashtable_s
{
  union
  {
    struct
    {
      size_t num_buckets;
      bucket_t* table;
      size_t hash;
      size_t version;
      uint8_t next_cache_line[CACHE_LINE_SIZE - (3 * sizeof(size_t)) - (sizeof(void*))];
      struct clht_hashtable_s* table_tmp;
      struct clht_hashtable_s* table_prev;
      struct clht_hashtable_s* table_new;
      volatile uint32_t num_expands;
      union
      {
	volatile uint32_t num_expands_threshold;
	uint32_t num_buckets_prev;
      };
      volatile int32_t is_helper;
      volatile int32_t helper_done;
      size_t version_min;
    };
    uint8_t padding[2*CACHE_LINE_SIZE];
  };
} clht_hashtable_t;
#pragma GCC diagnostic pop

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
typedef struct ALIGNED(CACHE_LINE_SIZE) ht_ts
{
  union
  {
    struct
    {
      size_t version;
      clht_hashtable_t* versionp;
      int id;
      volatile struct ht_ts* next;
    };
    uint8_t padding[CACHE_LINE_SIZE];
  };
} ht_ts_t;
#pragma GCC diagnostic pop


static inline int
is_power_of_two (unsigned int x) 
{
  return ((x != 0) && !(x & (x - 1)));
}

/** Jenkins' hash function for 64-bit integers. */
static inline uint64_t
__ac_Jenkins_hash_64(uint64_t key)
{
  key += ~(key << 32);
  key ^= (key >> 22);
  key += ~(key << 13);
  key ^= (key >> 8);
  key += (key << 3);
  key ^= (key >> 15);
  key += ~(key << 27);
  key ^= (key >> 31);
  return key;
}

/* Hash a key for a particular hashtable. */
uint32_t clht_hash(clht_hashtable_t* hashtable, clht_addr_t key );

static inline void
_mm_pause_rep(uint64_t w)
{
  while (w--)
    {
      _mm_pause();
    }
}

#if defined(XEON) | defined(COREi7) 
#  define TAS_RLS_MFENCE() _mm_sfence();
#elif defined(__tile__)
#  define TAS_RLS_MFENCE() _mm_mfence();
#else
#  define TAS_RLS_MFENCE()
#endif

#define LOCK_FREE   0
#define LOCK_UPDATE 1
#define LOCK_RESIZE 2

#if CLHT_USE_RTM == 1		/* USE RTM */
#  define LOCK_ACQ(lock, ht)			\
  lock_acq_rtm_chk_resize(lock, ht)
#  define LOCK_RLS(lock)			\
  if (likely(*(lock) == LOCK_FREE))		\
    {						\
      _xend();					\
      DPP(put_num_failed_on_new);		\
    }						\
  else						\
    {						\
      TAS_RLS_MFENCE();				\
     *lock = LOCK_FREE;				\
      DPP(put_num_failed_expand);		\
    }
#else  /* NO RTM */
#  define LOCK_ACQ(lock, ht)			\
  lock_acq_chk_resize(lock, ht)

#  define LOCK_RLS(lock)			\
  TAS_RLS_MFENCE();				\
 *lock = 0;	  

#endif	/* RTM */

#define LOCK_ACQ_RES(lock)			\
  lock_acq_resize(lock)

#define TRYLOCK_ACQ(lock)			\
  TAS_U8(lock)

#define TRYLOCK_RLS(lock)			\
  lock = LOCK_FREE

void ht_resize_help(clht_hashtable_t* h);

#if defined(DEBUG)
extern __thread uint32_t put_num_restarts;
#endif

static inline int
lock_acq_chk_resize(clht_lock_t* lock, clht_hashtable_t* h)
{
  char once = 1;
  clht_lock_t l;
  while ((l = CAS_U8(lock, LOCK_FREE, LOCK_UPDATE)) == LOCK_UPDATE)
    {
      if (once)
      	{
      	  DPP(put_num_restarts);
      	  once = 0;
      	}
      _mm_pause();
    }

  if (l == LOCK_RESIZE)
    {
      /* helping with the resize */
#if CLHT_HELP_RESIZE == 1
      ht_resize_help(h);
#endif

      while (h->table_new == NULL)
	{
	  _mm_pause();
	  _mm_mfence();
	}

      return 0;
    }

  return 1;
}

static inline int
lock_acq_resize(clht_lock_t* lock)
{
  clht_lock_t l;
  while ((l = CAS_U8(lock, LOCK_FREE, LOCK_RESIZE)) == LOCK_UPDATE)
    {
      _mm_pause();
    }

  if (l == LOCK_RESIZE)
    {
      return 0;
    }

  return 1;
}


/* ******************************************************************************** */
#if CLHT_USE_RTM == 1  /* use RTM */
/* ******************************************************************************** */

#include <immintrin.h>		/*  */

static inline int
lock_acq_rtm_chk_resize(clht_lock_t* lock, clht_hashtable_t* h)
{

  int rtm_retries = 1;
  do 
    {
      /* while (unlikely(*lock == LOCK_UPDATE)) */
      /* 	{ */
      /* 	  _mm_pause(); */
      /* 	} */

      if (likely(_xbegin() == _XBEGIN_STARTED))
	{
	  clht_lock_t lv = *lock;
	  if (likely(lv == LOCK_FREE))
	    {
	      return 1;
	    }
	  else if (lv == LOCK_RESIZE)
	    {
	      _xend();
#  if CLHT_HELP_RESIZE == 1
	      ht_resize_help(h);
#  endif

	      while (h->table_new == NULL)
		{
		  _mm_mfence();
		}

	      return 0;
	    }

	  DPP(put_num_restarts);
	  _xabort(0xff);
	}
    } while (rtm_retries-- > 0);

  return lock_acq_chk_resize(lock, h);
}
#endif	/* RTM */

/// Round up to next higher power of 2 (return x if it's already a power
/// of 2) for 32-bit numbers
static inline uint32_t pow2roundup (uint32_t x){
  if (x==0) return 1;
  --x;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  return x+1;
}

// Ticks
typedef uint64_t ticks;

static inline ticks getticks(void) {
#if defined(__i386__)
    ticks ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
    return ret;
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
#elif defined(__sparc__)
    ticks ret = 0;
    __asm__ __volatile__ ("rd %%tick, %0" : "=r" (ret) : "0" (ret));
    return ret;
#elif defined(__tile__)
    return get_cycle_count();
#else
    return 0;
#endif
}


/* ******************************************************************************** */
/* intefance */
/* ******************************************************************************** */

/* Create a new hashtable. */
clht_hashtable_t* clht_hashtable_create(uint32_t num_buckets);
clht_t* clht_create(uint32_t num_buckets);

/* Insert a key-value pair into a hashtable if the key doesn't exist. */
int clht_put(clht_t* hashtable, clht_addr_t key, clht_val_t val);

/* Insert a key-value pair into a hashtable. Return old value if it existed */
clht_val_t clht_set(clht_t* hashtable, clht_addr_t key, clht_val_t val);

/* Retrieve a key-value pair from a hashtable. */
clht_val_t clht_get(clht_hashtable_t* hashtable, clht_addr_t key, const char* full_key, const size_t key_size);

/* Remove a key-value pair from a hashtable. */
clht_val_t clht_remove(clht_t* hashtable, clht_addr_t key, const char* full_key, const size_t key_size);

size_t clht_size(clht_hashtable_t* hashtable);
size_t clht_size_mem(clht_hashtable_t* hashtable);
size_t clht_size_mem_garbage(clht_hashtable_t* hashtable);

void clht_gc_thread_init(clht_t* hashtable, int id);
void clht_gc_thread_version_max(void);
void clht_gc_thread_version(clht_hashtable_t* h);
int clht_gc_get_id(void);
int clht_gc_collect(clht_t* h);
int clht_gc_release(clht_hashtable_t* h);
int clht_gc_collect_all(clht_t* h);
int clht_gc_free(clht_hashtable_t* hashtable);
void clht_gc_destroy(clht_t* hashtable);
size_t clht_gc_min_version_used(clht_t* h);

void clht_print(clht_hashtable_t* hashtable);
#if defined(CLHT_LB_LINKED)
/* emergency_increase, grabs the lock and forces an increase by *emergency_increase times */
size_t ht_status(clht_t* hashtable, int resize_increase, int emergency_increase, int just_print);
#else
size_t ht_status(clht_t* hashtable, int resize_increase, int just_print);
#endif
bucket_t* clht_bucket_create(void);
int ht_resize_pes(clht_t* hashtable, int is_increase, int by);

const char* clht_type_desc(void);

#endif /* _CLHT_RES_RES_H_ */

