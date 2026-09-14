#!/usr/bin/env python3
"""Deterministic RTSP/TCP lock-cascade regression test for the Streaming plugin."""

import argparse
import http.client
import json
import os
import pathlib
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time


SDP = (
    "v=0\r\n"
    "o=- 1 1 IN IP4 127.0.0.1\r\n"
    "s=Local RTSP test\r\n"
    "c=IN IP4 127.0.0.1\r\n"
    "t=0 0\r\n"
    "a=control:*\r\n"
    "m=video 0 RTP/AVP 96\r\n"
    "a=rtpmap:96 H264/90000\r\n"
    "a=fmtp:96 packetization-mode=1;profile-level-id=42e01f\r\n"
    "a=control:trackID=0\r\n"
).encode()


class RtspServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), RtspHandler)
        self.play_count = 0
        self.play_condition = threading.Condition()
        self.keepalive_counts = {}
        self.keepalive_condition = threading.Condition()
        self.options_burst = threading.Event()

    def played(self):
        with self.play_condition:
            self.play_count += 1
            self.play_condition.notify_all()

    def wait_for_plays(self, expected, timeout):
        deadline = time.monotonic() + timeout
        with self.play_condition:
            while self.play_count < expected:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.play_condition.wait(remaining)
            return True

    def kept_alive(self, session_id):
        with self.keepalive_condition:
            self.keepalive_counts[session_id] = self.keepalive_counts.get(session_id, 0) + 1
            self.keepalive_condition.notify_all()

    def wait_for_keepalives(self, connections, per_connection, timeout):
        deadline = time.monotonic() + timeout
        with self.keepalive_condition:
            while True:
                counts = sorted(self.keepalive_counts.values(), reverse=True)
                if len(counts) >= connections and min(counts[:connections]) >= per_connection:
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.keepalive_condition.wait(remaining)


class RtspHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(0.05)
        pending = b""
        playing = False
        sequence = 1
        timestamp = 0
        next_packet = time.monotonic()
        session_id = f"local-{self.request.fileno()}"
        recent_options = []
        while True:
            if playing and time.monotonic() >= next_packet:
                rtp = struct.pack("!BBHII", 0x80, 96, sequence, timestamp, 1) + b"\x65\x00"
                frame = b"$\x00" + struct.pack("!H", len(rtp)) + rtp
                try:
                    self.request.sendall(frame)
                except OSError:
                    return
                sequence = (sequence + 1) & 0xFFFF
                timestamp = (timestamp + 9000) & 0xFFFFFFFF
                next_packet = time.monotonic() + 0.1
            try:
                chunk = self.request.recv(8192)
                if not chunk:
                    return
                pending += chunk
            except socket.timeout:
                continue
            except OSError:
                return

            while pending:
                if pending.startswith(b"$"):
                    if len(pending) < 4:
                        break
                    frame_length = struct.unpack("!H", pending[2:4])[0]
                    if len(pending) < 4 + frame_length:
                        break
                    pending = pending[4 + frame_length :]
                    continue
                header_end = pending.find(b"\r\n\r\n")
                if header_end < 0:
                    break
                request = pending[: header_end + 4]
                pending = pending[header_end + 4 :]
                lines = request.decode("ascii", "replace").split("\r\n")
                method, uri, _ = lines[0].split(" ", 2)
                headers = {}
                for line in lines[1:]:
                    if ":" in line:
                        key, value = line.split(":", 1)
                        headers[key.lower()] = value.strip()
                cseq = headers.get("cseq", "1")
                response_headers = [f"CSeq: {cseq}", f"Session: {session_id};timeout=2"]
                body = b""
                if method == "OPTIONS":
                    if playing:
                        self.server.kept_alive(session_id)
                        now = time.monotonic()
                        recent_options = [stamp for stamp in recent_options if now - stamp < 0.2]
                        recent_options.append(now)
                        if len(recent_options) >= 3:
                            self.server.options_burst.set()
                            playing = False
                    response_headers.append("Public: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN")
                elif method == "DESCRIBE":
                    body = SDP
                    response_headers.extend(
                        [
                            "Content-Type: application/sdp",
                            f"Content-Base: {uri.rstrip('/')}/",
                        ]
                    )
                elif method == "SETUP":
                    transport = headers.get(
                        "transport", "RTP/AVP/TCP;unicast;interleaved=0-1"
                    )
                    response_headers.append(f"Transport: {transport}")
                elif method == "PLAY":
                    playing = True
                    self.server.played()
                elif method == "TEARDOWN":
                    playing = False
                response_headers.append(f"Content-Length: {len(body)}")
                response = (
                    "RTSP/1.0 200 OK\r\n"
                    + "\r\n".join(response_headers)
                    + "\r\n\r\n"
                ).encode() + body
                try:
                    self.request.sendall(response)
                except OSError:
                    return


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def post_json(port, path, payload, timeout=2.0):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    body = json.dumps(payload)
    try:
        connection.request("POST", path, body, {"Content-Type": "application/json"})
        response = connection.getresponse()
        raw = response.read()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {raw[:200]!r}")
        return json.loads(raw)
    finally:
        connection.close()


def get_events(port, session_id, timeout=1.0):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    path = f"/janus/{session_id}?rid={int(time.time() * 1000)}&maxev=10"
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        raw = response.read()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {raw[:200]!r}")
        result = json.loads(raw)
        return result if isinstance(result, list) else [result]
    finally:
        connection.close()


def transaction(prefix):
    return f"{prefix}-{time.monotonic_ns()}"


def create_session(port, timeout=2.0):
    response = post_json(
        port, "/janus", {"janus": "create", "transaction": transaction("create")}, timeout
    )
    if response.get("janus") != "success":
        raise AssertionError(f"session creation failed: {response}")
    return response["data"]["id"]


def attach_streaming(port, session_id, timeout=2.0):
    response = post_json(
        port,
        f"/janus/{session_id}",
        {
            "janus": "attach",
            "plugin": "janus.plugin.streaming",
            "transaction": transaction("attach"),
        },
        timeout,
    )
    if response.get("janus") != "success":
        raise AssertionError(f"attach failed: {response}")
    return response["data"]["id"]


def plugin_message(port, session_id, handle_id, body, timeout=2.0):
    return post_json(
        port,
        f"/janus/{session_id}/{handle_id}",
        {"janus": "message", "body": body, "transaction": transaction(body["request"])},
        timeout,
    )


def wait_for_plugin_result(port, session_id, key, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            events = get_events(port, session_id, min(0.6, deadline - time.monotonic()))
        except (TimeoutError, socket.timeout):
            continue
        for event in events:
            data = event.get("plugindata", {}).get("data", {})
            result = data.get("result", data)
            if key in result:
                return result
    return None


def wait_for_api(port, process, timeout=8.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Janus exited early with status {process.returncode}")
        try:
            return create_session(port, timeout=0.3)
        except (ConnectionError, TimeoutError, socket.timeout, OSError):
            time.sleep(0.05)
    raise RuntimeError("Janus HTTP API did not become ready")


def expect_timeout(function, label):
    try:
        function()
    except (TimeoutError, socket.timeout):
        return
    raise AssertionError(f"{label} unexpectedly completed")


def write_configs(root, plugin_library, transport_library, http_port, rtsp_port):
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
        '  server_name = "local-rtsp-regression"\n'
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
        "admin: { admin_http = false admin_https = false }\n",
        encoding="utf-8",
    )
    streams = []
    for number in (1, 2):
        streams.append(
            f"stream-{number}: {{\n"
            '  type = "rtsp"\n'
            f"  id = {number}\n"
            f'  description = "Local stream {number}"\n'
            f'  url = "rtsp://127.0.0.1:{rtsp_port}/stream{number}"\n'
            "  audio = false\n"
            "  video = true\n"
            "  videopt = 96\n"
            '  videocodec = "h264"\n'
            "  rtsp_tcp = true\n"
            "  rtsp_failcheck = false\n"
            "  rtsp_reconnect_delay = 1\n"
            "  rtsp_timeout = 1\n"
            "  rtsp_conn_timeout = 1\n"
            "  threads = 0\n"
            "}\n"
        )
    (config_dir / "janus.plugin.streaming.jcfg").write_text(
        "general: { events = false }\n" + "".join(streams), encoding="utf-8"
    )
    return config_dir


def run(args):
    janus = pathlib.Path(args.janus).resolve()
    plugin = pathlib.Path(args.plugin).resolve()
    transport = pathlib.Path(args.transport).resolve()
    source_dir = pathlib.Path(__file__).resolve().parent
    for required in (janus, plugin, transport):
        if not required.exists():
            raise FileNotFoundError(required)

    with tempfile.TemporaryDirectory(prefix="janus-rtsp-e2e-") as temporary:
        root = pathlib.Path(temporary)
        shim = root / "force_rtcp_eagain.so"
        subprocess.run(
            [
                os.environ.get("CC", "cc"),
                "-shared",
                "-fPIC",
                "-Wall",
                "-Wextra",
                "-Werror",
                str(source_dir / "force_rtcp_eagain.c"),
                "-ldl",
                "-o",
                str(shim),
            ],
            check=True,
        )
        rtsp = RtspServer()
        rtsp_thread = threading.Thread(target=rtsp.serve_forever, daemon=True)
        rtsp_thread.start()
        http_port = free_port()
        config_dir = write_configs(
            root, plugin, transport, http_port, rtsp.server_address[1]
        )
        log_path = root / "janus.log"
        environment = os.environ.copy()
        environment["LD_PRELOAD"] = str(shim)
        environment["JANUS_TEST_FORCE_RTCP_EAGAIN"] = "1"
        trigger_path = root / "enable-rtcp-eagain"
        environment["JANUS_TEST_RTCP_EAGAIN_FILE"] = str(trigger_path)
        with log_path.open("wb") as log_file:
            process = subprocess.Popen(
                [str(janus), "-F", str(config_dir), "-o"],
                cwd=root,
                env=environment,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            clean_shutdown = False
            try:
                session_id = wait_for_api(http_port, process)
                handle_id = attach_streaming(http_port, session_id)
                if not rtsp.wait_for_plays(2, 8.0):
                    raise AssertionError(f"only {rtsp.play_count}/2 RTSP sources reached PLAY")

                watch = plugin_message(
                    http_port, session_id, handle_id, {"request": "watch", "id": 1}
                )
                if watch.get("janus") != "ack":
                    raise AssertionError(f"watch was not acknowledged: {watch}")
                if wait_for_plugin_result(http_port, session_id, "status", 3.0) is None:
                    raise AssertionError("watch event was not delivered")

                # Keep startup traffic outside the injected fault and ensure the
                # PLI one-second rate limiter cannot defer the switch request.
                time.sleep(1.1)
                trigger_path.touch()

                switched = plugin_message(
                    http_port, session_id, handle_id, {"request": "switch", "id": 2}
                )
                if switched.get("janus") != "ack":
                    raise AssertionError(f"switch was not acknowledged: {switched}")

                switch_result = wait_for_plugin_result(
                    http_port, session_id, "switched", 2.0
                )
                if args.expect == "fixed":
                    if switch_result is None:
                        raise AssertionError("fixed plugin did not complete switch")
                    listed = plugin_message(
                        http_port,
                        session_id,
                        handle_id,
                        {"request": "list"},
                        timeout=1.0,
                    )
                    if listed.get("janus") != "success":
                        raise AssertionError(f"list failed after switch: {listed}")
                    second_session = create_session(http_port, timeout=1.0)
                    attach_streaming(http_port, second_session, timeout=1.0)
                    create_session(http_port, timeout=1.0)
                    deadline = time.monotonic() + 3.0
                    while time.monotonic() < deadline:
                        log_file.flush()
                        if "[rtcp-eagain-shim]" in log_path.read_text(
                            encoding="utf-8", errors="replace"
                        ):
                            break
                        time.sleep(0.05)
                    else:
                        raise AssertionError("RTCP fault injection was not exercised")
                    if not rtsp.wait_for_keepalives(2, 2, 3.0):
                        raise AssertionError(
                            f"RTSP keepalive cycles were not sent: {rtsp.keepalive_counts}"
                        )
                    if rtsp.options_burst.wait(0.3):
                        if not rtsp.wait_for_plays(3, 3.0):
                            raise AssertionError(
                                "RTSP OPTIONS burst stopped media without reconnecting"
                            )
                        raise AssertionError(
                            "RTSP keepalive left the handle in OPTIONS mode "
                            "and triggered a reconnect"
                        )
                    continuity = plugin_message(
                        http_port,
                        session_id,
                        handle_id,
                        {"request": "list"},
                        timeout=1.0,
                    )
                    mountpoints = continuity.get("plugindata", {}).get("data", {}).get(
                        "list", []
                    )
                    ages = [
                        media["age_ms"]
                        for mountpoint in mountpoints
                        for media in mountpoint.get("media", [])
                        if "age_ms" in media
                    ]
                    if len(ages) != 2 or max(ages) > 500:
                        raise AssertionError(
                            f"RTSP media was not fresh after keepalive: {ages}"
                        )
                    if rtsp.play_count != 2:
                        raise AssertionError(
                            f"RTSP sources reconnected unexpectedly: {rtsp.play_count} PLAYs"
                        )
                else:
                    if switch_result is not None:
                        raise AssertionError("vulnerable plugin unexpectedly completed switch")
                    expect_timeout(
                        lambda: plugin_message(
                            http_port,
                            session_id,
                            handle_id,
                            {"request": "list"},
                            timeout=0.7,
                        ),
                        "list after blocked switch",
                    )
                    second_session = create_session(http_port, timeout=1.0)
                    expect_timeout(
                        lambda: attach_streaming(
                            http_port, second_session, timeout=0.7
                        ),
                        "attach behind sessions_mutex",
                    )
                    expect_timeout(
                        lambda: create_session(http_port, timeout=0.7),
                        "request behind blocked central thread",
                    )

                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=4.0 if args.expect == "fixed" else 1.5)
                    clean_shutdown = True
                except subprocess.TimeoutExpired:
                    clean_shutdown = False
                if args.expect == "fixed" and not clean_shutdown:
                    raise AssertionError("fixed Janus did not shut down within four seconds")
                if args.expect == "vulnerable" and clean_shutdown:
                    raise AssertionError("vulnerable Janus unexpectedly shut down cleanly")
            except BaseException:
                log_file.flush()
                tail = log_path.read_text(encoding="utf-8", errors="replace")[-8000:]
                print("--- Janus E2E failure log tail ---", file=sys.stderr)
                print(tail, file=sys.stderr)
                raise
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=3.0)
                rtsp.shutdown()
                rtsp.server_close()

        log_text = log_path.read_text(encoding="utf-8", errors="replace")
        if "[rtcp-eagain-shim]" not in log_text:
            raise AssertionError("fault injection marker absent from Janus log")
        if args.expect == "vulnerable" and "Ending requests thread" not in log_text:
            raise AssertionError("vulnerable shutdown did not reach the expected wait point")
        verified = "switch/list/attach/shutdown"
        if args.expect == "fixed":
            verified = "switch/list/attach/keepalive/media/shutdown"
        print(f"RTSP/TCP E2E {args.expect}: {verified} behavior verified")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--janus", required=True)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--transport", required=True)
    parser.add_argument("--expect", choices=("fixed", "vulnerable"), default="fixed")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
