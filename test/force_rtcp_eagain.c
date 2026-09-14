#define _GNU_SOURCE

#include <curl/curl.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#undef curl_easy_getinfo

typedef ssize_t (*send_function)(int, const void *, size_t, int);
typedef ssize_t (*recv_function)(int, void *, size_t, int);
typedef CURLcode (*getinfo_function)(CURL *, CURLINFO, ...);

static int observed_rtsp_fd = -1;
static int observed_stream1_fd = -1;

static int fault_is_active(void) {
	const char *enabled = getenv("JANUS_TEST_FORCE_RTCP_EAGAIN");
	const char *trigger = getenv("JANUS_TEST_RTCP_EAGAIN_FILE");
	return enabled != NULL && enabled[0] == '1' &&
		(trigger == NULL || access(trigger, F_OK) == 0);
}

static int receive_fault_is_active(void) {
	const char *enabled = getenv("JANUS_TEST_FORCE_RTSP_RECV_ERROR");
	const char *trigger = getenv("JANUS_TEST_RTSP_RECV_ERROR_FILE");
	return enabled != NULL && enabled[0] == '1' && trigger != NULL &&
		access(trigger, F_OK) == 0;
}

static int contains_bytes(const unsigned char *bytes, size_t length,
		const char *needle) {
	size_t needle_length = strlen(needle);
	if(bytes == NULL || needle_length == 0 || length < needle_length)
		return 0;
	for(size_t index = 0; index <= length - needle_length; index++) {
		if(memcmp(bytes + index, needle, needle_length) == 0)
			return 1;
	}
	return 0;
}

static int is_rtsp_request(const unsigned char *bytes, size_t length) {
	static const char *methods[] = {
		"OPTIONS ", "DESCRIBE ", "SETUP ", "PLAY ", "TEARDOWN "
	};
	for(size_t index = 0; index < sizeof(methods) / sizeof(methods[0]); index++) {
		size_t method_length = strlen(methods[index]);
		if(length >= method_length && memcmp(bytes, methods[index], method_length) == 0)
			return 1;
	}
	return 0;
}

ssize_t send(int fd, const void *buffer, size_t length, int flags) {
	static send_function real_send = NULL;
	static int reported = 0;
	if(real_send == NULL)
		real_send = (send_function)dlsym(RTLD_NEXT, "send");
	const unsigned char *bytes = buffer;
	if(bytes != NULL && is_rtsp_request(bytes, length)) {
		__atomic_store_n(&observed_rtsp_fd, fd, __ATOMIC_RELAXED);
		if(contains_bytes(bytes, length, "/stream1"))
			__atomic_store_n(&observed_stream1_fd, fd, __ATOMIC_RELAXED);
	}
	if(fault_is_active() && bytes != NULL && length >= 4 &&
			bytes[0] == '$' && (bytes[1] & 1) == 1) {
		if(__sync_bool_compare_and_swap(&reported, 0, 1)) {
			static const char marker[] = "[rtcp-eagain-shim] forced RTCP EAGAIN\n";
			(void)write(STDERR_FILENO, marker, sizeof(marker) - 1);
		}
		errno = EAGAIN;
		return -1;
	}
	if(real_send == NULL) {
		errno = ENOSYS;
		return -1;
	}
	return real_send(fd, buffer, length, flags);
}

ssize_t recv(int fd, void *buffer, size_t length, int flags) {
	static recv_function real_recv = NULL;
	static int reported = 0;
	if(real_recv == NULL)
		real_recv = (recv_function)dlsym(RTLD_NEXT, "recv");
	int stream1_fd = __atomic_load_n(&observed_stream1_fd, __ATOMIC_RELAXED);
	if(receive_fault_is_active() && fd == stream1_fd &&
			__sync_bool_compare_and_swap(&reported, 0, 1)) {
		static const char marker[] =
			"[rtsp-recv-shim] forced RTSP receive error\n";
		(void)write(STDERR_FILENO, marker, sizeof(marker) - 1);
		errno = ECONNRESET;
		return -1;
	}
	if(real_recv == NULL) {
		errno = ENOSYS;
		return -1;
	}
	return real_recv(fd, buffer, length, flags);
}

CURLcode curl_easy_getinfo(CURL *handle, CURLINFO info, ...) {
	static getinfo_function real_getinfo = NULL;
	if(real_getinfo == NULL)
		real_getinfo = (getinfo_function)dlsym(RTLD_NEXT, "curl_easy_getinfo");
	va_list arguments;
	va_start(arguments, info);
	void *output = va_arg(arguments, void *);
	va_end(arguments);
	if(info == CURLINFO_ACTIVESOCKET && fault_is_active()) {
		int fd = __atomic_load_n(&observed_rtsp_fd, __ATOMIC_RELAXED);
		if(fd >= 0) {
			*(curl_socket_t *)output = (curl_socket_t)fd;
			return CURLE_OK;
		}
	}
	if(real_getinfo == NULL)
		return CURLE_FAILED_INIT;
	return real_getinfo(handle, info, output);
}
