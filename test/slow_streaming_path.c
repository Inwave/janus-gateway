#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct _GMainContext GMainContext;
typedef struct _NiceAgent NiceAgent;
typedef void (*wakeup_function)(GMainContext *);
typedef int (*nice_send_function)(NiceAgent *, unsigned int, unsigned int,
	unsigned int, const char *);

static int named_thread(const char *prefix) {
	char name[16] = { 0 };
	if(pthread_getname_np(pthread_self(), name, sizeof(name)) != 0)
		return 0;
	return strncmp(name, prefix, strlen(prefix)) == 0;
}

static long delay_us(const char *delay_name, const char *trigger_name) {
	const char *configured = getenv(delay_name);
	const char *trigger = getenv(trigger_name);
	if(configured == NULL || trigger == NULL || access(trigger, F_OK) != 0)
		return 0;
	char *end = NULL;
	errno = 0;
	long delay = strtol(configured, &end, 10);
	if(errno != 0 || end == configured || *end != '\0' || delay <= 0)
		return 0;
	return delay;
}

static void delay(long microseconds) {
	struct timespec duration = {
		.tv_sec = microseconds / 1000000,
		.tv_nsec = (microseconds % 1000000) * 1000
	};
	while(nanosleep(&duration, &duration) < 0 && errno == EINTR) {
	}
}

void g_main_context_wakeup(GMainContext *context) {
	static wakeup_function real_wakeup = NULL;
	static int reported = 0;
	if(real_wakeup == NULL)
		real_wakeup = (wakeup_function)dlsym(RTLD_NEXT, "g_main_context_wakeup");
	if(real_wakeup != NULL)
		real_wakeup(context);
	long microseconds = delay_us(
		"JANUS_TEST_RELAY_DELAY_US", "JANUS_TEST_RELAY_DELAY_FILE");
	if(microseconds == 0 ||
			(!named_thread("mp ") && !named_thread("help ")))
		return;
	if(__sync_bool_compare_and_swap(&reported, 0, 1)) {
		static const char marker[] =
			"[relay-delay-shim] slowing viewer handoff\n";
		(void)write(STDERR_FILENO, marker, sizeof(marker) - 1);
	}
	delay(microseconds);
}

int nice_agent_send(NiceAgent *agent, unsigned int stream_id,
		unsigned int component_id, unsigned int length, const char *buffer) {
	static nice_send_function real_send = NULL;
	static int reported = 0;
	if(real_send == NULL)
		real_send = (nice_send_function)dlsym(RTLD_NEXT, "nice_agent_send");
	if(real_send == NULL)
		return -1;
	long microseconds = delay_us(
		"JANUS_TEST_OUTBOUND_DELAY_US", "JANUS_TEST_OUTBOUND_DELAY_FILE");
	if(microseconds > 0 && named_thread("hloop ")) {
		if(__sync_bool_compare_and_swap(&reported, 0, 1)) {
			static const char marker[] =
				"[outbound-delay-shim] slowing WebRTC socket handoff\n";
			(void)write(STDERR_FILENO, marker, sizeof(marker) - 1);
		}
		delay(microseconds);
	}
	return real_send(agent, stream_id, component_id, length, buffer);
}
