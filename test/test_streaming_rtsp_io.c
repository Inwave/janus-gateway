#include "../src/plugins/janus_streaming_rtsp_io.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int64_t monotonic_ms(void) {
	struct timespec now = { 0 };
	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return ((int64_t)now.tv_sec * 1000) + (now.tv_nsec / 1000000);
}

static void test_successful_send(void) {
	int sockets[2] = { -1, -1 };
	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
	const char frame[] = "$\001\000\014test-payload";
	assert(janus_streaming_rtsp_send_with_timeout(sockets[0], frame,
			sizeof(frame), 100) == 0);
	char received[sizeof(frame)] = { 0 };
	assert(recv(sockets[1], received, sizeof(received), 0) == sizeof(received));
	assert(memcmp(frame, received, sizeof(frame)) == 0);
	close(sockets[0]);
	close(sockets[1]);
}

static void test_saturated_socket_times_out(void) {
	int sockets[2] = { -1, -1 };
	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
	int flags = fcntl(sockets[0], F_GETFL, 0);
	assert(flags >= 0);
	assert(fcntl(sockets[0], F_SETFL, flags | O_NONBLOCK) == 0);
	char fill[4096] = { 0 };
	while(send(sockets[0], fill, sizeof(fill), 0) > 0) {
	}
	assert(errno == EAGAIN || errno == EWOULDBLOCK);

	int64_t started = monotonic_ms();
	errno = 0;
	assert(janus_streaming_rtsp_send_with_timeout(sockets[0], "x", 1, 50) == -1);
	int saved_errno = errno;
	int64_t elapsed = monotonic_ms() - started;
	assert(saved_errno == ETIMEDOUT);
	assert(elapsed >= 35);
	assert(elapsed < 500);
	close(sockets[0]);
	close(sockets[1]);
}

static void test_reconnect_jitter(void) {
	const int64_t delay = 15 * 1000 * 1000LL;
	int64_t first = janus_streaming_rtsp_reconnect_jitter("mountpoint-a", delay);
	int64_t repeated = janus_streaming_rtsp_reconnect_jitter("mountpoint-a", delay);
	int64_t second = janus_streaming_rtsp_reconnect_jitter("mountpoint-b", delay);
	assert(first == repeated);
	assert(first >= 0 && first <= 3 * 1000 * 1000LL);
	assert(second >= 0 && second <= 3 * 1000 * 1000LL);
	assert(first != second);
	assert(janus_streaming_rtsp_reconnect_jitter("mountpoint-a", 0) == 0);
	assert(janus_streaming_rtsp_reconnect_jitter(NULL, delay) == 0);
	const int64_t timer = 1000;
	assert(janus_streaming_rtsp_reconnect_schedule(
			timer + delay, timer, delay, first) == 0);
	const int64_t detected = timer + delay + 1;
	assert(janus_streaming_rtsp_reconnect_schedule(
			detected, timer, delay, first) == detected + first);
}

int main(void) {
	test_successful_send();
	test_saturated_socket_times_out();
	test_reconnect_jitter();
	puts("streaming RTSP I/O tests passed");
	return 0;
}
