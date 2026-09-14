#!/usr/bin/env python3
"""Separate slow WebRTC output from Streaming relay helper pressure."""

import argparse
import asyncio
import collections
import json
import os
import pathlib
import select
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time

from streaming_rtsp_tcp_e2e import (
    attach_streaming,
    create_session,
    free_port,
    get_events,
    plugin_message,
    plugin_result,
    post_json,
    transaction,
    wait_for_api,
)


SDP = (
    "v=0\r\n"
    "o=- 1 1 IN IP4 127.0.0.1\r\n"
    "s=Relay pressure test\r\n"
    "c=IN IP4 127.0.0.1\r\n"
    "t=0 0\r\n"
    "a=control:*\r\n"
    "m=video 0 RTP/AVP 96\r\n"
    "a=rtpmap:96 H264/90000\r\n"
    "a=fmtp:96 packetization-mode=1;profile-level-id=42e01f\r\n"
    "a=control:trackID=0\r\n"
).encode()


class PressureRtspServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, packets_per_second, payload_size):
        super().__init__(("127.0.0.1", 0), PressureRtspHandler)
        self.packet_interval = 1.0 / packets_per_second
        self.timestamp_step = max(1, 90000 // packets_per_second)
        self.payload_size = payload_size
        self.sent_counts = collections.Counter()
        self.play_counts = collections.Counter()
        self.pli_counts = collections.Counter()
        self.lock = threading.Lock()

    def sent(self, stream_name):
        with self.lock:
            self.sent_counts[stream_name] += 1

    def played(self, stream_name):
        with self.lock:
            self.play_counts[stream_name] += 1

    def feedback(self, stream_name, packet):
        if len(packet) >= 2 and packet[1] == 206 and packet[0] & 0x1F == 1:
            with self.lock:
                self.pli_counts[stream_name] += 1

    def feedback_snapshot(self):
        with self.lock:
            return dict(self.pli_counts)

    def snapshot(self):
        with self.lock:
            return dict(self.sent_counts), dict(self.play_counts)

    def wait_for_plays(self, expected, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            _, plays = self.snapshot()
            if sum(plays.values()) >= expected:
                return True
            time.sleep(0.05)
        return False


class PressureRtspHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        self.request.setblocking(False)
        incoming = b""
        outgoing = collections.deque()
        stream_name = None
        playing = False
        sequence = 1
        timestamp = 0
        next_packet = time.monotonic()
        session_id = f"pressure-{self.request.fileno()}"

        def queue_response(method, uri, headers):
            nonlocal next_packet, playing, stream_name
            cseq = headers.get("cseq", "1")
            response_headers = [f"CSeq: {cseq}", f"Session: {session_id};timeout=60"]
            body = b""
            if method == "OPTIONS":
                response_headers.append("Public: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN")
            elif method == "DESCRIBE":
                stream_name = uri.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
                body = SDP
                response_headers.extend(
                    ["Content-Type: application/sdp", f"Content-Base: {uri.rstrip('/')}/"]
                )
            elif method == "SETUP":
                transport = headers.get(
                    "transport", "RTP/AVP/TCP;unicast;interleaved=0-1"
                )
                response_headers.append(f"Transport: {transport}")
            elif method == "PLAY":
                playing = True
                next_packet = time.monotonic()
                self.server.played(stream_name)
            elif method == "TEARDOWN":
                playing = False
            response_headers.append(f"Content-Length: {len(body)}")
            response = (
                "RTSP/1.0 200 OK\r\n" + "\r\n".join(response_headers) + "\r\n\r\n"
            ).encode() + body
            outgoing.append([response, 0, False])

        while True:
            now = time.monotonic()
            if playing and not outgoing and now >= next_packet:
                payload = b"\x41" + bytes(self.server.payload_size - 1)
                rtp = struct.pack("!BBHII", 0x80, 96, sequence, timestamp, 1) + payload
                frame = b"$\x00" + struct.pack("!H", len(rtp)) + rtp
                outgoing.append([frame, 0, True])
                next_packet = now + self.server.packet_interval

            readable = [self.request]
            writable = [self.request] if outgoing else []
            try:
                ready_read, ready_write, _ = select.select(
                    readable, writable, [], min(self.server.packet_interval, 0.01)
                )
            except (OSError, ValueError):
                return

            if ready_write:
                chunk, offset, media = outgoing[0]
                try:
                    sent = self.request.send(chunk[offset:])
                except BlockingIOError:
                    sent = 0
                except OSError:
                    return
                if sent > 0:
                    offset += sent
                    outgoing[0][1] = offset
                if offset == len(chunk):
                    outgoing.popleft()
                    if media:
                        self.server.sent(stream_name)
                        sequence = (sequence + 1) & 0xFFFF
                        timestamp = (
                            timestamp + self.server.timestamp_step
                        ) & 0xFFFFFFFF

            if ready_read:
                try:
                    chunk = self.request.recv(8192)
                except BlockingIOError:
                    chunk = None
                except OSError:
                    return
                if chunk == b"":
                    return
                if chunk:
                    incoming += chunk
                while incoming:
                    if incoming.startswith(b"$"):
                        if len(incoming) < 4:
                            break
                        frame_length = struct.unpack("!H", incoming[2:4])[0]
                        if len(incoming) < 4 + frame_length:
                            break
                        channel = incoming[1]
                        packet = incoming[4 : 4 + frame_length]
                        incoming = incoming[4 + frame_length :]
                        if channel % 2 == 1:
                            self.server.feedback(stream_name, packet)
                        continue
                    header_end = incoming.find(b"\r\n\r\n")
                    if header_end < 0:
                        break
                    request = incoming[: header_end + 4]
                    incoming = incoming[header_end + 4 :]
                    lines = request.decode("ascii", "replace").split("\r\n")
                    method, uri, _ = lines[0].split(" ", 2)
                    headers = {}
                    for line in lines[1:]:
                        if ":" in line:
                            key, value = line.split(":", 1)
                            headers[key.lower()] = value.strip()
                    queue_response(method, uri, headers)


def write_configs(
    root, plugin_library, transport_library, http_port, admin_port, rtsp_port
):
    config_dir = root / "config"
    plugin_dir = root / "plugins"
    transport_dir = root / "transports"
    event_dir = root / "events"
    logger_dir = root / "loggers"
    for directory in (config_dir, plugin_dir, transport_dir, event_dir, logger_dir):
        directory.mkdir()
    os.symlink(plugin_library, plugin_dir / "libjanus_streaming.so")
    os.symlink(transport_library, transport_dir / "libjanus_http.so")
    (config_dir / "janus.jcfg").write_text(
        "general: {\n"
        f'  configs_folder = "{config_dir}"\n'
        f'  plugins_folder = "{plugin_dir}"\n'
        f'  transports_folder = "{transport_dir}"\n'
        f'  events_folder = "{event_dir}"\n'
        f'  loggers_folder = "{logger_dir}"\n'
        '  interface = "127.0.0.1"\n'
        "  session_timeout = 60\n"
        "  debug_level = 4\n"
        "  debug_colors = false\n"
        "}\n",
        encoding="utf-8",
    )
    (config_dir / "janus.transport.http.jcfg").write_text(
        "general: {\n"
        "  http = true\n"
        '  ip = "127.0.0.1"\n'
        f"  port = {http_port}\n"
        "  https = false\n"
        "}\n"
        "admin: {\n"
        "  admin_http = true\n"
        '  admin_ip = "127.0.0.1"\n'
        f"  admin_port = {admin_port}\n"
        "  admin_https = false\n"
        "}\n",
        encoding="utf-8",
    )
    streams = []
    for helpers in (0, 1, 2):
        stream_id = helpers + 1
        streams.append(
            f"stream-{helpers}: {{\n"
            '  type = "rtsp"\n'
            f"  id = {stream_id}\n"
            f'  description = "Relay helpers {helpers}"\n'
            f'  url = "rtsp://127.0.0.1:{rtsp_port}/stream{helpers}"\n'
            "  audio = false\n"
            "  video = true\n"
            "  videopt = 96\n"
            '  videocodec = "h264"\n'
            "  rtsp_tcp = true\n"
            "  rtsp_failcheck = true\n"
            "  rtsp_reconnect_delay = 30\n"
            "  rtsp_timeout = 2\n"
            "  rtsp_conn_timeout = 2\n"
            f"  threads = {helpers}\n"
            "}\n"
        )
    (config_dir / "janus.plugin.streaming.jcfg").write_text(
        "general: {\n"
        "  events = false\n"
        "  helper_queue_max_packets = 32\n"
        "  helper_queue_max_bytes = 65536\n"
        "  helper_queue_max_age_ms = 2000\n"
        "  helper_queue_global_max_bytes = 65536\n"
        "}\n"
        + "".join(streams),
        encoding="utf-8",
    )
    return config_dir


async def post_async(port, path, payload, timeout=3.0):
    return await asyncio.to_thread(post_json, port, path, payload, timeout)


async def wait_for_jsep(port, session_id, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            events = await asyncio.to_thread(
                get_events, port, session_id, min(0.6, deadline - time.monotonic())
            )
        except (TimeoutError, socket.timeout):
            continue
        for event in events:
            if event.get("jsep"):
                return event["jsep"]
    raise TimeoutError("Streaming offer was not delivered")


async def start_viewer(port, mountpoint_id):
    from aiortc import RTCPeerConnection, RTCSessionDescription

    session_id = await asyncio.to_thread(create_session, port, 3.0)
    handle_id = await asyncio.to_thread(attach_streaming, port, session_id, 3.0)
    watch = await post_async(
        port,
        f"/janus/{session_id}/{handle_id}",
        {
            "janus": "message",
            "body": {"request": "watch", "id": mountpoint_id},
            "transaction": transaction("pressure-watch"),
        },
    )
    if watch.get("janus") != "ack":
        raise AssertionError(f"watch {mountpoint_id} failed: {watch}")
    offer = await wait_for_jsep(port, session_id)
    pc = RTCPeerConnection()
    await pc.setRemoteDescription(
        RTCSessionDescription(sdp=offer["sdp"], type=offer["type"])
    )
    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)
    started = await post_async(
        port,
        f"/janus/{session_id}/{handle_id}",
        {
            "janus": "message",
            "body": {"request": "start"},
            "jsep": {
                "type": pc.localDescription.type,
                "sdp": pc.localDescription.sdp,
                "trickle": False,
            },
            "transaction": transaction("pressure-start"),
        },
    )
    if started.get("janus") != "ack":
        await pc.close()
        raise AssertionError(f"start {mountpoint_id} failed: {started}")
    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        if pc.connectionState == "connected":
            return session_id, handle_id, pc
        if pc.connectionState in ("failed", "closed"):
            break
        await asyncio.sleep(0.05)
    state = pc.connectionState
    await pc.close()
    raise AssertionError(f"viewer {mountpoint_id} did not connect: {state}")


async def received_packets(pc):
    stats = await pc.getStats()
    return sum(
        report.packetsReceived
        for report in stats.values()
        if report.type == "inbound-rtp" and report.kind == "video"
    )


async def queued_packets(admin_port, viewer):
    session_id, handle_id, _ = viewer
    response = await post_async(
        admin_port,
        f"/admin/{session_id}/{handle_id}",
        {
            "janus": "handle_info",
            "transaction": transaction("pressure-info"),
        },
    )
    if response.get("janus") != "success":
        raise AssertionError(f"handle_info failed: {response}")
    return response.get("info", {}).get("queued-packets", -1)


async def helper_queue_info(port, viewer, mountpoint_id):
    session_id, handle_id, _ = viewer
    response = await asyncio.to_thread(
        plugin_message,
        port,
        session_id,
        handle_id,
        {"request": "info", "id": mountpoint_id},
        2.0,
    )
    return plugin_result(response).get("info", {}).get("helper_queue")


def packet_deltas(before, after):
    return {
        helpers: after.get(f"stream{helpers}", 0)
        - before.get(f"stream{helpers}", 0)
        for helpers in (0, 1, 2)
    }


async def phase_snapshot(viewers, rtsp):
    sent, plays = rtsp.snapshot()
    received = {
        helpers: [await received_packets(pc) for _, _, pc in group]
        for helpers, group in viewers.items()
    }
    return sent, plays, received


async def measure_phase(
    viewers, rtsp, admin_port, trigger_path, duration, sample_queues=False
):
    initial_sent, _, initial_received = await phase_snapshot(viewers, rtsp)
    maximum_queues = {helpers: [0, 0] for helpers in (0, 1, 2)}
    trigger_path.touch()
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        await asyncio.sleep(min(0.2, deadline - time.monotonic()))
        if sample_queues:
            for helpers, group in viewers.items():
                current = await asyncio.gather(
                    *(queued_packets(admin_port, viewer) for viewer in group)
                )
                maximum_queues[helpers] = [
                    max(previous, observed)
                    for previous, observed in zip(maximum_queues[helpers], current)
                ]
    trigger_path.unlink(missing_ok=True)
    final_sent, plays, final_received = await phase_snapshot(viewers, rtsp)
    received_delta = {
        helpers: [
            final - initial
            for final, initial in zip(final_received[helpers], initial_received[helpers])
        ]
        for helpers in (0, 1, 2)
    }
    return (
        packet_deltas(initial_sent, final_sent),
        received_delta,
        maximum_queues,
        plays,
    )


async def exercise(
    port, admin_port, relay_trigger_path, outbound_trigger_path, rtsp, duration
):
    viewers = {helpers: [] for helpers in (0, 1, 2)}
    try:
        for helpers in (0, 1, 2):
            viewers[helpers] = await asyncio.gather(
                start_viewer(port, helpers + 1),
                start_viewer(port, helpers + 1),
            )
        await asyncio.sleep(1.0)

        outbound_sent, outbound_received, outbound_queues, plays = await measure_phase(
            viewers,
            rtsp,
            admin_port,
            outbound_trigger_path,
            duration,
            sample_queues=True,
        )
        if any(plays.get(f"stream{helpers}") != 1 for helpers in (0, 1, 2)):
            raise AssertionError(f"RTSP source reconnected with slow viewers: {plays}")
        if min(outbound_sent.values()) < max(outbound_sent.values()) * 0.9:
            raise AssertionError(
                "slow WebRTC output changed RTSP draining according to helper count: "
                f"sent={outbound_sent}"
            )
        if any(
            max(outbound_received[helpers]) > outbound_sent[helpers] * 0.8
            for helpers in (0, 1, 2)
        ):
            raise AssertionError(
                "outbound delay did not make the viewers observably slow: "
                f"sent={outbound_sent}, received={outbound_received}"
            )
        if any(max(outbound_queues[helpers]) < 2 for helpers in (0, 1, 2)):
            raise AssertionError(
                "slow output did not create backpressure in the core queues: "
                f"queues={outbound_queues}"
            )
        outbound_rates = [
            packets
            for helpers in (0, 1, 2)
            for packets in outbound_received[helpers]
        ]
        if min(outbound_rates) < max(outbound_rates) * 0.8:
            raise AssertionError(
                "helper count changed slow-viewer throughput unexpectedly: "
                f"received={outbound_received}"
            )

        await asyncio.sleep(2.0)
        initial_plis = rtsp.feedback_snapshot()
        relay_sent, relay_received, _, plays = await measure_phase(
            viewers, rtsp, admin_port, relay_trigger_path, duration
        )
        if any(plays.get(f"stream{helpers}") != 1 for helpers in (0, 1, 2)):
            raise AssertionError(f"RTSP source reconnected under relay pressure: {plays}")
        if relay_sent[1] < relay_sent[0] * 1.4:
            raise AssertionError(
                "one helper did not isolate RTSP receive: "
                f"sent={relay_sent}, received={relay_received}"
            )
        if min(relay_received[2]) < max(relay_received[1]) * 1.35:
            raise AssertionError(
                "two helpers did not improve two-viewer delivery: "
                f"received={relay_received}"
            )
        if relay_sent[2] < relay_sent[1] * 0.85:
            raise AssertionError(f"two helpers reduced source draining: sent={relay_sent}")
        helper_queues = {
            helpers: await helper_queue_info(
                port, viewers[helpers][0], helpers + 1
            )
            for helpers in (0, 1, 2)
        }
        if helper_queues[0] is not None:
            raise AssertionError(
                f"threads=0 unexpectedly exposed helper metrics: {helper_queues[0]}"
            )
        for helpers in (1, 2):
            queue = helper_queues[helpers]
            if queue is None:
                raise AssertionError(f"threads={helpers} did not expose helper metrics")
            if (
                queue.get("max_packets_per_helper") != 32
                or queue.get("max_bytes_per_helper") != 65536
                or queue.get("max_age_ms") != 2000
                or queue.get("global_max_bytes") != 65536
            ):
                raise AssertionError(f"unexpected helper limits: {queue}")
            if helpers == 1 and queue.get("dropped_packets", 0) < 1:
                raise AssertionError(
                    f"threads={helpers} did not shed a slow helper backlog: {queue}"
                )
            if queue.get("current_packets", 0) > helpers * 32:
                raise AssertionError(f"helper packet limit was exceeded: {queue}")
            if queue.get("current_bytes", 0) > helpers * 65536:
                raise AssertionError(f"helper byte limit was exceeded: {queue}")
            if queue.get("oldest_age_ms", 0) > 2000:
                raise AssertionError(f"helper age limit was exceeded: {queue}")
            if queue.get("global_current_bytes", 0) > 65536:
                raise AssertionError(f"global helper byte limit was exceeded: {queue}")
            if queue.get("global_peak_bytes", 0) > 65536:
                raise AssertionError(f"global helper peak limit was exceeded: {queue}")
            for entry in queue.get("helpers", []):
                if entry.get("peak_packets", 0) > 32:
                    raise AssertionError(f"helper peak packet limit was exceeded: {entry}")
                if entry.get("peak_bytes", 0) > 65536:
                    raise AssertionError(f"helper peak byte limit was exceeded: {entry}")
            if helpers == 1 and queue.get("packet_limit_hits", 0) < 1:
                raise AssertionError(f"helper packet limit was not reached: {queue}")
            if helpers == 1 and not any(
                entry.get("dropped_oldest_packets", 0) > 0
                for entry in queue.get("helpers", [])
            ):
                raise AssertionError(f"helper did not discard its oldest backlog: {queue}")
        await asyncio.sleep(1.2)
        final_plis = rtsp.feedback_snapshot()
        for helpers in (1,):
            stream = f"stream{helpers}"
            if final_plis.get(stream, 0) <= initial_plis.get(stream, 0):
                raise AssertionError(
                    "dropping delayed video did not request a new keyframe: "
                    f"before={initial_plis}, after={final_plis}"
                )
        return {
            "slow_outbound": {
                "source_packets": outbound_sent,
                "viewer_packets": outbound_received,
                "maximum_core_queues": outbound_queues,
            },
            "slow_relay": {
                "source_packets": relay_sent,
                "viewer_packets": relay_received,
                "helper_queues": helper_queues,
                "pli_before": initial_plis,
                "pli_after": final_plis,
            },
        }
    finally:
        await asyncio.gather(
            *(pc.close() for group in viewers.values() for _, _, pc in group),
            return_exceptions=True,
        )


def run(args):
    try:
        import aiortc  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("streaming relay pressure E2E requires aiortc") from exc
    janus = pathlib.Path(args.janus).resolve()
    plugin = pathlib.Path(args.plugin).resolve()
    transport = pathlib.Path(args.transport).resolve()
    source_dir = pathlib.Path(__file__).resolve().parent
    for required in (janus, plugin, transport):
        if not required.exists():
            raise FileNotFoundError(required)

    with tempfile.TemporaryDirectory(prefix="janus-relay-pressure-") as temporary:
        root = pathlib.Path(temporary)
        shim = root / "slow_relay_wakeup.so"
        subprocess.run(
            [
                os.environ.get("CC", "cc"),
                "-shared",
                "-fPIC",
                "-Wall",
                "-Wextra",
                "-Werror",
                str(source_dir / "slow_streaming_path.c"),
                "-ldl",
                "-pthread",
                "-o",
                str(shim),
            ],
            check=True,
        )
        rtsp = PressureRtspServer(args.packets_per_second, args.payload_size)
        rtsp_thread = threading.Thread(target=rtsp.serve_forever, daemon=True)
        rtsp_thread.start()
        http_port = free_port()
        admin_port = free_port()
        config_dir = write_configs(
            root, plugin, transport, http_port, admin_port, rtsp.server_address[1]
        )
        log_path = root / "janus.log"
        relay_trigger_path = root / "slow-viewer-handoff"
        outbound_trigger_path = root / "slow-webrtc-output"
        environment = os.environ.copy()
        environment["LD_PRELOAD"] = str(shim)
        environment["JANUS_TEST_RELAY_DELAY_US"] = str(
            args.relay_delay_ms * 1000
        )
        environment["JANUS_TEST_RELAY_DELAY_FILE"] = str(relay_trigger_path)
        environment["JANUS_TEST_OUTBOUND_DELAY_US"] = str(
            args.outbound_delay_ms * 1000
        )
        environment["JANUS_TEST_OUTBOUND_DELAY_FILE"] = str(outbound_trigger_path)
        with log_path.open("wb") as log_file:
            process = subprocess.Popen(
                [str(janus), "-F", str(config_dir), "-o"],
                cwd=root,
                env=environment,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            try:
                warm_session_id = wait_for_api(http_port, process)
                warm_handle_id = attach_streaming(http_port, warm_session_id, 3.0)
                if not warm_handle_id:
                    raise AssertionError("could not attach Streaming warm-up handle")
                if not rtsp.wait_for_plays(3, 8.0):
                    raise AssertionError(
                        f"RTSP sources did not start: {rtsp.snapshot()[1]}"
                    )
                measurements = asyncio.run(
                    exercise(
                        http_port,
                        admin_port,
                        relay_trigger_path,
                        outbound_trigger_path,
                        rtsp,
                        args.duration,
                    )
                )
            except BaseException:
                log_file.flush()
                print("--- Janus pressure E2E failure log tail ---", file=sys.stderr)
                print(
                    log_path.read_text(encoding="utf-8", errors="replace")[-8000:],
                    file=sys.stderr,
                )
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3.0)
                rtsp.shutdown()
                rtsp.server_close()
        log_text = log_path.read_text(encoding="utf-8", errors="replace")
        for marker in ("[outbound-delay-shim]", "[relay-delay-shim]"):
            if marker not in log_text:
                raise AssertionError(f"test fault was not injected: {marker}")
        print("Streaming slow-viewer E2E: " + json.dumps(measurements, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--janus", required=True)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--transport", required=True)
    parser.add_argument("--packets-per-second", type=int, default=70)
    parser.add_argument("--payload-size", type=int, default=1200)
    parser.add_argument("--relay-delay-ms", type=int, default=20)
    parser.add_argument("--outbound-delay-ms", type=int, default=40)
    parser.add_argument("--duration", type=float, default=8.0)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
