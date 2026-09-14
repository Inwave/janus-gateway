#ifndef JANUS_STREAMING_QUEUE_LIMITS_H
#define JANUS_STREAMING_QUEUE_LIMITS_H

#include <stdbool.h>
#include <stdint.h>

#include <glib.h>

typedef struct janus_streaming_queue_limits {
	uint32_t max_packets;
	uint64_t max_bytes;
	int64_t max_age_us;
} janus_streaming_queue_limits;

typedef struct janus_streaming_queue_budget {
	volatile gint current_bytes;
	volatile gint peak_bytes;
	gint max_bytes;
} janus_streaming_queue_budget;

bool janus_streaming_queue_bytes_fit(uint64_t max_bytes,
		uint64_t queued_bytes, uint64_t packet_bytes);

bool janus_streaming_queue_budget_reserve(janus_streaming_queue_budget *budget,
		guint bytes);
void janus_streaming_queue_budget_release(janus_streaming_queue_budget *budget,
		guint bytes);
gint janus_streaming_queue_budget_current(const janus_streaming_queue_budget *budget);
gint janus_streaming_queue_budget_peak(const janus_streaming_queue_budget *budget);

/* Whether adding a packet would exceed a helper queue limit. */
bool janus_streaming_queue_limit_reached(
		const janus_streaming_queue_limits *limits,
		uint32_t queued_packets, uint64_t queued_bytes,
		int64_t oldest_queued_at, uint64_t packet_bytes, int64_t now);

/* Remove the FIFO head while preserving a distinguished control item. */
gpointer janus_streaming_queue_take_oldest_unlocked(GAsyncQueue *queue,
		gconstpointer protected_item);

#endif
