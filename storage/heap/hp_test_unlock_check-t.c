/*
  Unit test: the unlock-time table verification must not run when the
  THR_LOCK is not held.

  ha_heap::external_lock(F_UNLCK) verifies the table with heap_check_heap().
  That is safe on the ordinary unlock path, where mysql_unlock_tables() calls
  unlock_external() before thr_multi_unlock() and the THR_LOCK is still held.
  It is not safe on either path that unlocks after a failed lock attempt: when
  mysql_lock_tables() balances the external locks it took because
  thr_multi_lock() timed out, and when lock_external() itself unwinds the
  tables it already locked because a later table refused.  In both the caller
  holds nothing, while another connection is writing.

  The test drives the real lock protocol (thr_multi_lock/thr_multi_unlock,
  exactly what sql/lock.cc calls) rather than reproducing a race by repetition.
  A holder thread takes TL_WRITE and keeps it until told to let go, so the
  contending request is guaranteed to time out; the holder also parks the share
  in the state a writer passes through mid-row, so the verification has
  something to (wrongly) find.  Every outcome here is forced, not raced.
*/

#include "hp_test_helpers.h"
#include <my_pthread.h>

/* Holder-thread handshake.  Both flags are set under state_mutex. */
static pthread_mutex_t state_mutex;
static pthread_cond_t state_cond;
static int holder_ready;                        /* lock taken, share parked */
static int holder_release;                      /* main is done, let go */

static HP_INFO *holder_info;

/*
  The slot the holder allocated but has not written yet.  Handed back to main
  so it can undo the parked state after the holder is gone.
*/
static uchar *parked_slot;


static void set_flag(int *flag)
{
  pthread_mutex_lock(&state_mutex);
  *flag= 1;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);
}


static void wait_flag(int *flag)
{
  pthread_mutex_lock(&state_mutex);
  while (!*flag)
    pthread_cond_wait(&state_cond, &state_mutex);
  pthread_mutex_unlock(&state_mutex);
}


/*
  Put a handle in the state the SQL layer leaves it in just before the
  THR_LOCK is attempted: ha_heap::store_lock() has recorded the requested type
  (at get_lock_data() time, well before any locking), and lock_external() has
  reached ha_heap::external_lock() with that type.  Nothing is held yet.
*/

static void request_lock(HP_INFO *info, enum thr_lock_type type)
{
  info->lock.type= type;                        /* ha_heap::store_lock() */
  hp_lock_request_begin(info);                  /* ha_heap::external_lock() */
}


/*
  Reproduce the state heap_write() is in between allocating a slot and marking
  it visible: next_free_record_pos() has already published the slot in
  total_records, but the record has not been stored yet.

  The slot is zeroed rather than left as it comes from my_malloc() so that the
  test asserts on a defined outcome.  A zero flags byte is what a scan of a
  half-written row legitimately sees; leaving the malloc garbage in place is
  what makes the same access an uninitialised read under MSAN.
*/

static uchar *park_mid_write(HP_SHARE *share)
{
  uchar *pos= next_free_record_pos(share);
  if (pos)
    memset(pos, 0, share->block.recbuffer);
  return pos;
}


static void unpark_mid_write(HP_SHARE *share, uchar *pos)
{
  hp_push_free_record(share, pos);
  hp_shrink_tail(share);
}


static void *holder_thread(void *arg __attribute__((unused)))
{
  THR_LOCK_INFO owner;
  THR_LOCK_DATA *lock_data[1];

  my_thread_init();
  thr_lock_info_init(&owner, my_thread_var);

  request_lock(holder_info, TL_WRITE);
  lock_data[0]= &holder_info->lock;

  if (thr_multi_lock(lock_data, 1, &owner, 0) != THR_LOCK_SUCCESS)
  {
    /* Nothing else holds the lock yet, so this cannot fail */
    set_flag(&holder_ready);
    my_thread_end();
    return NULL;
  }

  parked_slot= park_mid_write(holder_info->s);

  set_flag(&holder_ready);
  wait_flag(&holder_release);

  thr_multi_unlock(lock_data, 1, 0);
  my_thread_end();
  return NULL;
}


int main(int argc __attribute__((unused)),
         char **argv __attribute__((unused)))
{
  HP_SHARE *share, *share2;
  HP_INFO *info1, *info2, *other;
  THR_LOCK_INFO waiter;
  THR_LOCK_DATA *waiter_lock_data[2];
  enum enum_thr_lock_result lock_result;
  pthread_t holder;
  uchar rec[REC_LENGTH];
  uchar blob_data[200];
  int i;

  plan(15);
  MY_INIT("hp_test_unlock_check-t");
  pthread_mutex_init(&state_mutex, NULL);
  pthread_cond_init(&state_cond, NULL);

  if (create_and_open("test_unlock_check", &share, &info1) ||
      create_and_open("test_unlock_check2", &share2, &other))
  {
    ok(0, "setup failed");
    return exit_status();
  }

  info2= heap_open("test_unlock_check", 2);
  if (!info2)
  {
    ok(0, "second open failed");
    heap_close(info1);
    return exit_status();
  }
  heap_extra(info2, HA_EXTRA_NO_READCHECK);

  /* Populate, so that share->changed is set and the scan has work to do */
  for (i= 0; i < 5; i++)
  {
    memset(blob_data, 'a' + i, sizeof(blob_data));
    build_record(rec, 100 + i, blob_data, (uint16) sizeof(blob_data));
    if (heap_write(info1, rec))
    {
      ok(0, "populate failed");
      heap_close(info2);
      heap_drop_table(other);
      heap_drop_table(info1);
      return exit_status();
    }
  }

  ok(heap_check_heap(info1, 0) == 0, "table is consistent before the test");
  ok(share->changed != 0, "share is marked changed by the writes");

  holder_info= info1;
  pthread_create(&holder, NULL, holder_thread, NULL);
  wait_flag(&holder_ready);

  ok(parked_slot != NULL, "holder parked the share mid-write under TL_WRITE");

  /*
    What sql/lock.cc does: thr_multi_lock() with a timeout, which cannot
    succeed because the holder keeps TL_WRITE until we say so.
  */
  thr_lock_info_init(&waiter, my_thread_var);
  request_lock(info2, TL_WRITE);
  waiter_lock_data[0]= &info2->lock;
  lock_result= thr_multi_lock(waiter_lock_data, 1, &waiter, 1);

  ok(lock_result == THR_LOCK_WAIT_TIMEOUT, "contending lock request times out");

  /*
    mysql_lock_tables() now calls unlock_external() to balance the external
    locks it took, reaching ha_heap::external_lock(F_UNLCK) with no THR_LOCK.
    thr_multi_lock() left the request marked TL_UNLOCK, which is how the
    handler tells this path from an ordinary unlock.
  */
  ok(info2->lock.type == TL_UNLOCK,
     "failed lock request is left marked TL_UNLOCK");
  ok(!hp_lock_is_held(info2), "the failed requester knows it holds no lock");
  ok(hp_lock_is_held(info1), "the holder knows it does hold the lock");
  ok(!hp_may_check_heap_on_unlock(info2),
     "unlock-time verification is skipped when the lock is not held");

  /*
    Show what running it there would have cost.  The holder is still parked
    mid-write, so the verification finds the half-written slot and marks the
    share crashed -- which is what makes a later INSERT fail with
    ER_NOT_KEYFILE even though nothing is wrong with the table.
  */
  ok(heap_check_heap(info2, 0) != 0 && heap_is_crashed(share),
     "running it anyway reports damage and marks the share crashed");

  heap_clear_state(share);

  /*
    A real statement locks more than one table.  With two tables in one
    request, whichever the sort in thr_multi_lock() puts first, the other
    table is either never attempted or granted and then rolled back -- and a
    granted lock is reset by thr_unlock(), not by the loop that normalizes
    the requests thr_multi_lock() never got to.  Neither handle may claim the
    lock afterwards.
  */
  request_lock(info2, TL_WRITE);
  request_lock(other, TL_WRITE);
  waiter_lock_data[0]= &info2->lock;
  waiter_lock_data[1]= &other->lock;
  lock_result= thr_multi_lock(waiter_lock_data, 2, &waiter, 1);

  ok(lock_result == THR_LOCK_WAIT_TIMEOUT,
     "multi-table request times out on the contended table");
  ok(!hp_lock_is_held(info2) && !hp_lock_is_held(other),
     "neither table of a failed multi-table request claims the lock");

  /* An uncontended grant, to show the predicate does say yes when it should */
  request_lock(other, TL_WRITE);
  waiter_lock_data[0]= &other->lock;
  lock_result= thr_multi_lock(waiter_lock_data, 1, &waiter, 1);

  ok(lock_result == THR_LOCK_SUCCESS && hp_lock_is_held(other),
     "an uncontended grant is reported as held");

  thr_multi_unlock(waiter_lock_data, 1, 0);
  ok(!hp_lock_is_held(other), "the lock is not reported as held once released");

  /*
    The third F_UNLCK path: lock_external() locked this table, a later table
    refused, and lock_external() unwinds what it had already locked --
    entirely before thr_multi_lock() runs.  The requested type is set and
    nothing normalized it, so the type alone cannot tell this apart from an
    ordinary unlock.  Only the fact that no grant ever arrived can.
  */
  request_lock(other, TL_WRITE);
  ok(!hp_lock_is_held(other),
     "a request unwound before the lock is attempted claims nothing");

  /* Let the holder finish its row and release the lock */
  set_flag(&holder_release);
  pthread_join(holder, NULL);

  unpark_mid_write(share, parked_slot);

  ok(heap_check_heap(info1, 0) == 0,
     "the table was consistent all along: the report was a false positive");

  heap_close(info2);
  heap_drop_table(other);
  heap_drop_table(info1);
  pthread_cond_destroy(&state_cond);
  pthread_mutex_destroy(&state_mutex);
  my_end(0);
  return exit_status();
}
