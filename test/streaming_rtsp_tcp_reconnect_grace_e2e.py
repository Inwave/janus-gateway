#!/usr/bin/env python3
"""Regression test for RTP recovery during the RTSP/TCP reconnect grace period."""

import argparse
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

import streaming_rtsp_tcp_e2e as e2e


class RtspServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), RtspHandler)
        self.play_count = 0
        self.packet_count = 0
        self.rtcp_count = 0
        self.paused = False
        self.rtcp_pending = False
        self.condition = threading.Condition()

    def played(self):
        with self.condition:
            self.play_count += 1
            self.condition.notify_all()

    def packet_sent(self):
        with self.condition:
            self.packet_count += 1
            self.condition.notify_all()

    def rtcp_sent(self):
        with self.condition:
            self.rtcp_count += 1
            self.condition.notify_all()

    def set_paused(self, paused):
        with self.condition:
            self.paused = paused

    def request_rtcp(self):
        with self.condition:
            self.rtcp_pending = True

    def take_rtcp_request(self):
        with self.condition:
            if not self.rtcp_pending:
                return False
            self.rtcp_pending = False
            return True

    def wait_for(self, attribute, expected, timeout):
        deadline = time.monotonic() + timeout
        with self.condition:
            while getattr(self, attribute) < expected:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(remaining)
            return True


class RtspHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(0.05)
        pending = b""
        playing = False
        sequence = 1
        timestamp = 0
        next_packet = time.monotonic()
        session_id = f"grace-{self.request.fileno()}"

        while True:
            if playing and self.server.take_rtcp_request():
                rtcp = struct.pack("!BBHI", 0x80, 201, 1, 1)
                try:
                    self.request.sendall(b"$\x01" + struct.pack("!H", len(rtcp)) + rtcp)
                except OSError:
                    return
                self.server.rtcp_sent()

            if playing and not self.server.paused and time.monotonic() >= next_packet:
                rtp = struct.pack("!BBHII", 0x80, 96, sequence, timestamp, 1) + b"\x65\x00"
                try:
                    self.request.sendall(b"$\x00" + struct.pack("!H", len(rtp)) + rtp)
                except OSError:
                    return
                self.server.packet_sent()
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

                body = b""
                response_headers = [
                    f"CSeq: {headers.get('cseq', '1')}",
                    f"Session: {session_id};timeout=120",
                ]
                if method == "OPTIONS":
                    response_headers.append(
                        "Public: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN"
                    )
                elif method == "DESCRIBE":
                    body = e2e.SDP
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


def write_config(root, plugin, transport, http_port, rtsp_port):
    config_dir = e2e.write_configs(root, plugin, transport, http_port, rtsp_port)
    (config_dir / "janus.plugin.streaming.jcfg").write_text(
        "general: { events = false }\n"
        "stream-1: {\n"
        '  type = "rtsp"\n'
        "  id = 1\n"
        '  description = "Reconnect grace regression"\n'
        f'  url = "rtsp://127.0.0.1:{rtsp_port}/stream1"\n'
        "  audio = false\n"
        "  video = true\n"
        "  videopt = 96\n"
        '  videocodec = "h264"\n'
        "  rtsp_tcp = true\n"
        "  rtsp_failcheck = false\n"
        "  rtsp_reconnect_delay = 2\n"
        "  rtsp_session_timeout = 60\n"
        "  rtsp_timeout = 5\n"
        "  rtsp_conn_timeout = 1\n"
        "  threads = 0\n"
        "}\n",
        encoding="utf-8",
    )
    return config_dir


def run(args):
    janus = pathlib.Path(args.janus).resolve()
    plugin = pathlib.Path(args.plugin).resolve()
    transport = pathlib.Path(args.transport).resolve()
    for required in (janus, plugin, transport):
        if not required.exists():
            raise FileNotFoundError(required)

    with tempfile.TemporaryDirectory(prefix="janus-rtsp-grace-") as temporary:
        root = pathlib.Path(temporary)
        rtsp = RtspServer()
        rtsp_thread = threading.Thread(target=rtsp.serve_forever, daemon=True)
        rtsp_thread.start()
        http_port = e2e.free_port()
        config_dir = write_config(
            root, plugin, transport, http_port, rtsp.server_address[1]
        )
        log_path = root / "janus.log"
        with log_path.open("wb") as log_file:
            process = subprocess.Popen(
                [str(janus), "-F", str(config_dir), "-o"],
                cwd=root,
                env=os.environ.copy(),
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            try:
                session_id = e2e.wait_for_api(http_port, process)
                handle_id = e2e.attach_streaming(http_port, session_id)
                if not rtsp.wait_for("play_count", 1, 8.0):
                    raise AssertionError("RTSP source did not reach PLAY")
                if not rtsp.wait_for("packet_count", 3, 2.0):
                    raise AssertionError("RTSP source did not produce initial RTP")

                before = e2e.no_media_counts(
                    e2e.plugin_message(
                        http_port, session_id, handle_id, {"request": "list"}
                    )
                )
                next_rtcp = rtsp.rtcp_count + 1
                rtsp.set_paused(True)
                time.sleep(2.01)
                rtsp.request_rtcp()
                if not rtsp.wait_for("rtcp_count", next_rtcp, 1.0):
                    raise AssertionError("RTSP source did not emit the RTCP trigger")
                time.sleep(0.02)
                rtsp.set_paused(False)
                time.sleep(0.6)

                after_response = e2e.plugin_message(
                    http_port, session_id, handle_id, {"request": "list"}
                )
                after = e2e.no_media_counts(after_response)
                mountpoints = e2e.plugin_result(after_response).get("list", [])
                ages = [
                    media["age_ms"]
                    for mountpoint in mountpoints
                    for media in mountpoint.get("media", [])
                    if "age_ms" in media
                ]
                reconnected = rtsp.play_count != 1 or after.get(1) != before.get(1)
                if args.expect == "fixed" and reconnected:
                    raise AssertionError(
                        "media resumed during reconnect grace, but Janus reconnected: "
                        f"plays={rtsp.play_count}, before={before}, after={after}"
                    )
                if args.expect == "vulnerable" and not reconnected:
                    raise AssertionError("vulnerable Janus unexpectedly avoided reconnect")
                if args.expect == "fixed" and (not ages or max(ages) > 500):
                    raise AssertionError(f"resumed RTP was not consumed promptly: {ages}")

                process.send_signal(signal.SIGTERM)
                process.wait(timeout=4.0)
            except BaseException:
                log_file.flush()
                print("--- Janus reconnect-grace log tail ---", file=sys.stderr)
                print(
                    log_path.read_text(encoding="utf-8", errors="replace")[-8000:],
                    file=sys.stderr,
                )
                raise
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=3.0)
                rtsp.shutdown()
                rtsp.server_close()

    print(f"RTSP/TCP reconnect-grace E2E {args.expect}: behavior verified")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--janus", required=True)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--transport", required=True)
    parser.add_argument("--expect", choices=("fixed", "vulnerable"), default="fixed")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
