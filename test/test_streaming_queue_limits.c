#include "../src/plugins/janus_streaming_queue_limits.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>

static const janus_streaming_queue_limits limits = {
	.max_packets = 4,
	.max_bytes = 1000,
	.max_age_us = 1000000,
};

static void test_accepts_packet_within_all_limits(void) {
	assert(!janus_streaming_queue_limit_reached(
		&limits, 2, 400, 9500000, 200, 10000000));
}

static void test_rejects_packet_count_limit(void) {
	assert(janus_streaming_queue_limit_reached(
		&limits, 4, 400, 9500000, 200, 10000000));
}

static void test_rejects_byte_limit_and_oversized_packet(void) {
	assert(janus_streaming_queue_limit_reached(
		&limits, 2, 900, 9500000, 101, 10000000));
	assert(janus_streaming_queue_limit_reached(
		&limits, 0, 0, 0, 1001, 10000000));
	assert(janus_streaming_queue_limit_reached(
		&limits, 1, UINT64_MAX, 9500000, 1, 10000000));
}

static void test_global_byte_budget_boundary(void) {
	assert(janus_streaming_queue_bytes_fit(1000, 900, 100));
	assert(!janus_streaming_queue_bytes_fit(1000, 900, 101));
	assert(!janus_streaming_queue_bytes_fit(1000, 0, 1001));
	assert(!janus_streaming_queue_bytes_fit(UINT64_MAX, UINT64_MAX, 1));
}

static void test_global_budget_reservation_is_hard_bounded(void) {
	janus_streaming_queue_budget budget = { .max_bytes = 100 };
	assert(janus_streaming_queue_budget_reserve(&budget, 60));
	assert(!janus_streaming_queue_budget_reserve(&budget, 41));
	assert(janus_streaming_queue_budget_reserve(&budget, 40));
	assert(janus_streaming_queue_budget_current(&budget) == 100);
	assert(janus_streaming_queue_budget_peak(&budget) == 100);
	assert(!janus_streaming_queue_budget_reserve(&budget, 1));
	janus_streaming_queue_budget_release(&budget, 60);
	assert(janus_streaming_queue_budget_current(&budget) == 40);
	assert(janus_streaming_queue_budget_reserve(&budget, 60));
	assert(janus_streaming_queue_budget_peak(&budget) == 100);
	janus_streaming_queue_budget_release(&budget, 100);
	assert(janus_streaming_queue_budget_current(&budget) == 0);
}

typedef struct budget_worker {
	janus_streaming_queue_budget *budget;
	guint successes;
} budget_worker;

static gpointer reserve_concurrently(gpointer data) {
	budget_worker *worker = data;
	for(guint attempt = 0; attempt < 2000; attempt++) {
		if(janus_streaming_queue_budget_reserve(worker->budget, 1))
			worker->successes++;
	}
	return NULL;
}

static void test_global_budget_is_bounded_under_concurrency(void) {
	janus_streaming_queue_budget budget = { .max_bytes = 1000 };
	budget_worker workers[8] = { 0 };
	GThread *threads[8] = { 0 };
	for(guint index = 0; index < 8; index++) {
		workers[index].budget = &budget;
		threads[index] = g_thread_new("budget-test", reserve_concurrently, &workers[index]);
	}
	guint successes = 0;
	for(guint index = 0; index < 8; index++) {
		g_thread_join(threads[index]);
		successes += workers[index].successes;
	}
	assert(successes == 1000);
	assert(janus_streaming_queue_budget_current(&budget) == 1000);
	assert(janus_streaming_queue_budget_peak(&budget) == 1000);
	janus_streaming_queue_budget_release(&budget, 1000);
}

static void test_rejects_expired_oldest_packet(void) {
	assert(janus_streaming_queue_limit_reached(
		&limits, 1, 200, 9000000, 200, 10000000));
	assert(!janus_streaming_queue_limit_reached(
		&limits, 1, 200, 9000001, 200, 10000000));
}

static void test_invalid_limits_fail_closed(void) {
	janus_streaming_queue_limits invalid = limits;
	invalid.max_packets = 0;
	assert(janus_streaming_queue_limit_reached(
		&invalid, 0, 0, 0, 1, 10000000));
}

static void test_discards_fifo_head_and_preserves_shutdown(void) {
	GAsyncQueue *queue = g_async_queue_new();
	int shutdown = 0;
	g_async_queue_lock(queue);
	g_async_queue_push_unlocked(queue, GINT_TO_POINTER(1));
	g_async_queue_push_unlocked(queue, GINT_TO_POINTER(2));
	g_async_queue_push_unlocked(queue, GINT_TO_POINTER(3));
	assert(janus_streaming_queue_take_oldest_unlocked(queue, &shutdown) ==
		GINT_TO_POINTER(1));
	assert(g_async_queue_try_pop_unlocked(queue) == GINT_TO_POINTER(2));
	assert(g_async_queue_try_pop_unlocked(queue) == GINT_TO_POINTER(3));
	g_async_queue_push_unlocked(queue, &shutdown);
	assert(janus_streaming_queue_take_oldest_unlocked(queue, &shutdown) == NULL);
	assert(g_async_queue_try_pop_unlocked(queue) == &shutdown);
	g_async_queue_unlock(queue);
	g_async_queue_unref(queue);
}

static void test_reached_limit_evicts_oldest_and_keeps_newest(void) {
	janus_streaming_queue_limits two_packets = limits;
	two_packets.max_packets = 2;
	GAsyncQueue *queue = g_async_queue_new();
	int shutdown = 0;
	g_async_queue_lock(queue);
	g_async_queue_push_unlocked(queue, GINT_TO_POINTER(10));
	g_async_queue_push_unlocked(queue, GINT_TO_POINTER(20));
	assert(janus_streaming_queue_limit_reached(
		&two_packets, 2, 200, 9500000, 100, 10000000));
	assert(janus_streaming_queue_take_oldest_unlocked(queue, &shutdown) ==
		GINT_TO_POINTER(10));
	g_async_queue_push_unlocked(queue, GINT_TO_POINTER(30));
	assert(g_async_queue_try_pop_unlocked(queue) == GINT_TO_POINTER(20));
	assert(g_async_queue_try_pop_unlocked(queue) == GINT_TO_POINTER(30));
	g_async_queue_unlock(queue);
	g_async_queue_unref(queue);
}

int main(void) {
	test_accepts_packet_within_all_limits();
	test_rejects_packet_count_limit();
	test_rejects_byte_limit_and_oversized_packet();
	test_global_byte_budget_boundary();
	test_global_budget_reservation_is_hard_bounded();
	test_global_budget_is_bounded_under_concurrency();
	test_rejects_expired_oldest_packet();
	test_invalid_limits_fail_closed();
	test_discards_fifo_head_and_preserves_shutdown();
	test_reached_limit_evicts_oldest_and_keeps_newest();
	puts("streaming helper queue limit tests passed");
	return 0;
}
