#ifndef JANUS_STREAMING_RTSP_IO_H
#define JANUS_STREAMING_RTSP_IO_H

#include <stddef.h>
#include <stdint.h>

/* Send a complete RTSP interleaved frame without ever blocking past timeout_ms. */
int janus_streaming_rtsp_send_with_timeout(int fd, const void *buffer,
		size_t length, int timeout_ms);

/* Stable per-mountpoint jitter, capped at 20% of delay and three seconds. */
int64_t janus_streaming_rtsp_reconnect_jitter(const char *mountpoint_id,
		int64_t reconnect_delay);

/* Return the absolute retry time once the configured no-media floor expires. */
int64_t janus_streaming_rtsp_reconnect_schedule(int64_t now,
		int64_t reconnect_timer, int64_t reconnect_delay,
		int64_t reconnect_jitter);

#endif
