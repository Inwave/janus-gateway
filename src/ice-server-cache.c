#include "ice-server-cache.h"

void janus_ice_server_cache_set(janus_ice_server_cache *cache,
		const char *hostname, const char *address) {
	if(cache == NULL)
		return;
	g_mutex_lock(&cache->mutex);
	char *old_hostname = cache->hostname;
	char *old_address = cache->address;
	cache->hostname = g_strdup(hostname);
	cache->address = g_strdup(address);
	g_mutex_unlock(&cache->mutex);
	g_free(old_hostname);
	g_free(old_address);
}

char *janus_ice_server_cache_dup_hostname(janus_ice_server_cache *cache) {
	if(cache == NULL)
		return NULL;
	g_mutex_lock(&cache->mutex);
	char *hostname = g_strdup(cache->hostname);
	g_mutex_unlock(&cache->mutex);
	return hostname;
}

char *janus_ice_server_cache_dup_address(janus_ice_server_cache *cache) {
	if(cache == NULL)
		return NULL;
	g_mutex_lock(&cache->mutex);
	char *address = g_strdup(cache->address);
	g_mutex_unlock(&cache->mutex);
	return address;
}

gboolean janus_ice_server_cache_refresh(janus_ice_server_cache *cache,
		const char *hostname, const char *address) {
	if(cache == NULL || hostname == NULL || address == NULL || *address == '\0')
		return FALSE;
	gboolean changed = FALSE;
	char *old_address = NULL;
	g_mutex_lock(&cache->mutex);
	if(!g_strcmp0(cache->hostname, hostname) && g_strcmp0(cache->address, address)) {
		old_address = cache->address;
		cache->address = g_strdup(address);
		changed = TRUE;
	}
	g_mutex_unlock(&cache->mutex);
	g_free(old_address);
	return changed;
}

void janus_ice_server_cache_clear(janus_ice_server_cache *cache) {
	if(cache == NULL)
		return;
	g_mutex_lock(&cache->mutex);
	g_clear_pointer(&cache->hostname, g_free);
	g_clear_pointer(&cache->address, g_free);
	g_mutex_unlock(&cache->mutex);
	g_mutex_clear(&cache->mutex);
}
