#include "janus_streaming_queue_limits.h"

#include <stddef.h>

bool janus_streaming_queue_bytes_fit(uint64_t max_bytes,
		uint64_t queued_bytes, uint64_t packet_bytes) {
	return max_bytes > 0 && packet_bytes <= max_bytes &&
		queued_bytes <= max_bytes - packet_bytes;
}

bool janus_streaming_queue_budget_reserve(janus_streaming_queue_budget *budget,
		guint bytes) {
	if(budget == NULL || budget->max_bytes <= 0)
		return false;
	gint current = g_atomic_int_get(&budget->current_bytes);
	while(true) {
		if(!janus_streaming_queue_bytes_fit(budget->max_bytes, current, bytes))
			return false;
		gint updated = current + (gint)bytes;
		if(g_atomic_int_compare_and_exchange(&budget->current_bytes, current, updated)) {
			gint peak = g_atomic_int_get(&budget->peak_bytes);
			while(updated > peak && !g_atomic_int_compare_and_exchange(
					&budget->peak_bytes, peak, updated))
				peak = g_atomic_int_get(&budget->peak_bytes);
			return true;
		}
		current = g_atomic_int_get(&budget->current_bytes);
	}
}

void janus_streaming_queue_budget_release(janus_streaming_queue_budget *budget,
		guint bytes) {
	if(budget != NULL && bytes > 0)
		g_atomic_int_add(&budget->current_bytes, -(gint)bytes);
}

gint janus_streaming_queue_budget_current(const janus_streaming_queue_budget *budget) {
	return budget == NULL ? 0 : g_atomic_int_get(&budget->current_bytes);
}

gint janus_streaming_queue_budget_peak(const janus_streaming_queue_budget *budget) {
	return budget == NULL ? 0 : g_atomic_int_get(&budget->peak_bytes);
}

bool janus_streaming_queue_limit_reached(
		const janus_streaming_queue_limits *limits,
		uint32_t queued_packets, uint64_t queued_bytes,
		int64_t oldest_queued_at, uint64_t packet_bytes, int64_t now) {
	if(limits == NULL || limits->max_packets == 0 || limits->max_bytes == 0 ||
			limits->max_age_us <= 0)
		return true;
	if(queued_packets >= limits->max_packets)
		return true;
	if(!janus_streaming_queue_bytes_fit(limits->max_bytes, queued_bytes, packet_bytes))
		return true;
	if(oldest_queued_at > 0 && now >= oldest_queued_at &&
			now - oldest_queued_at >= limits->max_age_us)
		return true;
	return false;
}

gpointer janus_streaming_queue_take_oldest_unlocked(GAsyncQueue *queue,
		gconstpointer protected_item) {
	if(queue == NULL)
		return NULL;
	gpointer item = g_async_queue_try_pop_unlocked(queue);
	if(item == protected_item) {
		g_async_queue_push_front_unlocked(queue, item);
		return NULL;
	}
	return item;
}
