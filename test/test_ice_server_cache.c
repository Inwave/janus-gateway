#include "../src/ice-server-cache.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static void assert_cached_address(janus_ice_server_cache *cache, const char *expected) {
	char *address = janus_ice_server_cache_dup_address(cache);
	assert(address != NULL);
	assert(!strcmp(address, expected));
	g_free(address);
}

static void test_failure_and_timeout_keep_last_known_address(void) {
	janus_ice_server_cache cache = { 0 };
	janus_ice_server_cache_set(&cache, "turn.example.test", "192.0.2.10");

	assert(!janus_ice_server_cache_refresh(&cache, "turn.example.test", NULL));
	assert_cached_address(&cache, "192.0.2.10");
	assert(!janus_ice_server_cache_refresh(&cache, "turn.example.test", ""));
	assert_cached_address(&cache, "192.0.2.10");

	janus_ice_server_cache_clear(&cache);
}

static void test_success_replaces_cached_address(void) {
	janus_ice_server_cache cache = { 0 };
	janus_ice_server_cache_set(&cache, "stun.example.test", "192.0.2.20");

	assert(janus_ice_server_cache_refresh(&cache,
		"stun.example.test", "192.0.2.21"));
	assert_cached_address(&cache, "192.0.2.21");
	assert(!janus_ice_server_cache_refresh(&cache,
		"stun.example.test", "192.0.2.21"));
	assert_cached_address(&cache, "192.0.2.21");

	janus_ice_server_cache_clear(&cache);
}

static void test_late_result_for_old_hostname_is_ignored(void) {
	janus_ice_server_cache cache = { 0 };
	janus_ice_server_cache_set(&cache, "new.example.test", "192.0.2.30");

	assert(!janus_ice_server_cache_refresh(&cache,
		"old.example.test", "192.0.2.31"));
	assert_cached_address(&cache, "192.0.2.30");

	char *hostname = janus_ice_server_cache_dup_hostname(&cache);
	assert(hostname != NULL);
	assert(!strcmp(hostname, "new.example.test"));
	g_free(hostname);
	janus_ice_server_cache_clear(&cache);
}

typedef struct cache_reader {
	janus_ice_server_cache *cache;
	volatile gint stop;
} cache_reader;

static gpointer read_cache_concurrently(gpointer data) {
	cache_reader *reader = data;
	while(!g_atomic_int_get(&reader->stop)) {
		char *address = janus_ice_server_cache_dup_address(reader->cache);
		assert(address != NULL);
		assert(!strcmp(address, "192.0.2.40") || !strcmp(address, "192.0.2.41"));
		g_free(address);
	}
	return NULL;
}

static void test_refresh_is_safe_while_sessions_read_cache(void) {
	janus_ice_server_cache cache = { 0 };
	janus_ice_server_cache_set(&cache, "turn.example.test", "192.0.2.40");
	cache_reader reader = { .cache = &cache };
	GThread *thread = g_thread_new("ice-cache-reader", read_cache_concurrently, &reader);
	for(guint attempt = 0; attempt < 10000; attempt++) {
		const char *address = attempt % 2 ? "192.0.2.40" : "192.0.2.41";
		janus_ice_server_cache_refresh(&cache, "turn.example.test", address);
	}
	g_atomic_int_set(&reader.stop, 1);
	g_thread_join(thread);
	assert_cached_address(&cache, "192.0.2.40");
	janus_ice_server_cache_clear(&cache);
}

int main(void) {
	test_failure_and_timeout_keep_last_known_address();
	test_success_replaces_cached_address();
	test_late_result_for_old_hostname_is_ignored();
	test_refresh_is_safe_while_sessions_read_cache();
	puts("ICE server cache tests passed");
	return 0;
}
