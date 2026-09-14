# Janus testing

The files in this sub-folder are intended to be used for testing Janus.

## aiortc functional testing

We implemented some RTC Python clients based on [aiortc](https://github.com/aiortc/aiortc).
In order to use them you'll need Python >= 3.9.

To install required libraries:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### echo.py

This script does a quick echotest with a specified Janus instance.
The source code has been largely inspired by the examples on the aiortc repository.
The program basically initiates a Janus session through a WebSocket connection, then creates a new echotest handle and starts an audio/video negotiation according to the echotest API.

Once everything has been succesfully set up, the client waits for 5 seconds and then checks the following assertions:
* WebSocket is connected
* ICE Connection State is completed
* DTLS state is connected
* outbound RTP packets are greater or equal than inbound RTP packets

If any of these assertion fails, the client returns a non-zero value.
No assertion has been made on the RTP packets to check if they contains valid media or not, I guess this is something that might be added in future.

The script is invoked like this:

```bash
python3 echo.py ws://localhost:8188/ --play-from media_file --verbose
```

The websocket endpoint default is `ws://localhost:8188/`.
The media_file is optional and if omitted dummy audio/video tracks will be generated.

### The test_aiortc.sh helper script

We have added a `test_aiortc.sh` helper script in order to easily launch a Janus aiortc-based test.
The scripts must be invoked like this:

```bash
./test_aiortc.sh echo.py ws://localhost:8188/
```

It will start a Janus instance in the background taking the binary files from the Janus sources directory.
Then it will wait for some seconds before invoking the Python script specified in the first parameter.
Finally it will check the exit status of the Python script and kill the Janus instance.

## RTSP/TCP Streaming regression test

`streaming_rtsp_tcp_e2e.py` starts two loopback-only fake RTSP cameras and a
Janus instance. A local `LD_PRELOAD` shim forces `EAGAIN` only when the Streaming
plugin tries to send interleaved RTCP. The test verifies that `switch`, `list`, a
new session/attach and graceful shutdown remain bounded. It also uses a short
RTSP session timeout to prove that periodic keepalive `OPTIONS` requests are
sent without a burst, the easy handle returns to `RECEIVE`, media remains fresh
and no reconnect is triggered. It then pauses media on only one fake camera and
verifies that `rtsp_no_media_count` increments only for that mountpoint and has
the same value in the Streaming `list` and `info` responses. Finally, the shim
forces a real libcurl receive error on that source and verifies that the source
reconnects, media becomes fresh, the counter advances only once and the
operator-owned `enabled` state remains true. With `rtsp_notify_changes` enabled,
both recovery paths must also notify the affected subscriber with
`rtsp-disconnected` followed by `rtsp-reconnected`. The counter is cumulative
for the lifetime of the RTSP mountpoint.

After building Janus, run the fixed-code regression with:

```bash
make check-streaming-rtsp-e2e
```

The script also accepts `--expect vulnerable` so the same fault can document the
pre-fix lock cascade against an explicitly supplied older plugin build. It uses
only Python's standard library and a C compiler; all sockets bind to `127.0.0.1`.

## RTP/UDP Streaming compatibility test

`streaming_rtp_udp_e2e.py` starts Janus with one loopback RTP mountpoint, sends
real RTP packets over UDP and verifies fresh `age_ms` values in both `list` and
`info`, the absence of RTSP-only fields and graceful shutdown. This exercises
the UDP fallback in builds both with and without libcurl:

```bash
make check-streaming-rtp-e2e
```

When validating artifacts explicitly built without libcurl, invoke the script
with `--expect-no-curl` so it also checks the plugin's runtime capability
warning:

```bash
python3 test/streaming_rtp_udp_e2e.py \
  --janus /path/to/janus \
  --plugin /path/to/libjanus_streaming.so \
  --transport /path/to/libjanus_http.so \
  --expect-no-curl
```
