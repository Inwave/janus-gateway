#include "janus_streaming_rtsp_io.h"

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdint.h>
#include <sys/socket.h>
#include <time.h>

#define JANUS_STREAMING_RECONNECT_JITTER_CAP_US (3 * 1000 * 1000LL)

static int64_t janus_streaming_monotonic_ms(void) {
	struct timespec now = { 0 };
	if(clock_gettime(CLOCK_MONOTONIC, &now) < 0)
		return -1;
	return ((int64_t)now.tv_sec * 1000) + (now.tv_nsec / 1000000);
}

int janus_streaming_rtsp_send_with_timeout(int fd, const void *buffer,
		size_t length, int timeout_ms) {
	if(fd < 0 || buffer == NULL || length == 0 || timeout_ms <= 0) {
		errno = EINVAL;
		return -1;
	}
	int64_t started = janus_streaming_monotonic_ms();
	if(started < 0)
		return -1;
	int64_t deadline = started + timeout_ms;
	const unsigned char *position = buffer;
	size_t remaining = length;
	while(remaining > 0) {
		int64_t now = janus_streaming_monotonic_ms();
		if(now < 0)
			return -1;
		if(now >= deadline) {
			errno = ETIMEDOUT;
			return -1;
		}
		int flags = 0;
#ifdef MSG_NOSIGNAL
		flags |= MSG_NOSIGNAL;
#endif
#ifdef MSG_DONTWAIT
		flags |= MSG_DONTWAIT;
#endif
		ssize_t sent = send(fd, position, remaining, flags);
		if(sent > 0) {
			position += sent;
			remaining -= (size_t)sent;
			continue;
		}
		if(sent == 0) {
			errno = EPIPE;
			return -1;
		}
		if(errno == EINTR)
			continue;
		if(errno != EAGAIN && errno != EWOULDBLOCK)
			return -1;

		now = janus_streaming_monotonic_ms();
		if(now < 0)
			return -1;
		int64_t wait_ms = deadline - now;
		if(wait_ms <= 0) {
			errno = ETIMEDOUT;
			return -1;
		}
		struct pollfd writable = { .fd = fd, .events = POLLOUT, .revents = 0 };
		int poll_timeout = wait_ms > INT_MAX ? INT_MAX : (int)wait_ms;
		int polled = poll(&writable, 1, poll_timeout);
		if(polled == 0) {
			errno = ETIMEDOUT;
			return -1;
		}
		if(polled < 0) {
			if(errno == EINTR)
				continue;
			return -1;
		}
		if(writable.revents & POLLNVAL) {
			errno = EBADF;
			return -1;
		}
		if(writable.revents & (POLLERR | POLLHUP)) {
			errno = EPIPE;
			return -1;
		}
	}
	return 0;
}

int64_t janus_streaming_rtsp_reconnect_jitter(const char *mountpoint_id,
		int64_t reconnect_delay) {
	if(mountpoint_id == NULL || reconnect_delay <= 0)
		return 0;
	int64_t limit = reconnect_delay / 5;
	if(limit > JANUS_STREAMING_RECONNECT_JITTER_CAP_US)
		limit = JANUS_STREAMING_RECONNECT_JITTER_CAP_US;
	if(limit <= 0)
		return 0;

	/* FNV-1a gives a stable distribution without shared random state. */
	uint64_t hash = UINT64_C(14695981039346656037);
	const unsigned char *current = (const unsigned char *)mountpoint_id;
	while(*current != '\0') {
		hash ^= *current++;
		hash *= UINT64_C(1099511628211);
	}
	/* Avalanche adjacent numeric IDs before mapping them to a short interval. */
	hash ^= hash >> 33;
	hash *= UINT64_C(0xff51afd7ed558ccd);
	hash ^= hash >> 33;
	hash *= UINT64_C(0xc4ceb9fe1a85ec53);
	hash ^= hash >> 33;
	return (int64_t)(hash % ((uint64_t)limit + 1));
}

int64_t janus_streaming_rtsp_reconnect_schedule(int64_t now,
		int64_t reconnect_timer, int64_t reconnect_delay,
		int64_t reconnect_jitter) {
	if(now <= reconnect_timer || reconnect_delay < 0 || reconnect_jitter < 0)
		return 0;
	if(now - reconnect_timer <= reconnect_delay)
		return 0;
	if(reconnect_jitter > INT64_MAX - now)
		return INT64_MAX;
	return now + reconnect_jitter;
}
