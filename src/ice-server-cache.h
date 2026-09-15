#ifndef JANUS_ICE_SERVER_CACHE_H
#define JANUS_ICE_SERVER_CACHE_H

#include <glib.h>

typedef struct janus_ice_server_cache {
	GMutex mutex;
	char *hostname;
	char *address;
} janus_ice_server_cache;

void janus_ice_server_cache_set(janus_ice_server_cache *cache,
	const char *hostname, const char *address);
char *janus_ice_server_cache_dup_hostname(janus_ice_server_cache *cache);
char *janus_ice_server_cache_dup_address(janus_ice_server_cache *cache);

/* Applies a refresh only when it completed successfully for the hostname that
 * is still configured. A NULL address represents failure or timeout and must
 * leave the last known address untouched. */
gboolean janus_ice_server_cache_refresh(janus_ice_server_cache *cache,
	const char *hostname, const char *address);

void janus_ice_server_cache_clear(janus_ice_server_cache *cache);

#endif
