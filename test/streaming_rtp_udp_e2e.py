#!/usr/bin/env python3
"""Loopback RTP/UDP smoke test for Streaming builds with or without libcurl."""

import argparse
import http.client
import json
import os
import pathlib
import signal
import socket
import struct
import subprocess
import tempfile
import time


def free_port(socket_type):
    with socket.socket(socket.AF_INET, socket_type) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def post_json(port, path, payload, timeout=2.0):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        connection.request(
            "POST", path, json.dumps(payload), {"Content-Type": "application/json"}
        )
        response = connection.getresponse()
        raw = response.read()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {raw[:200]!r}")
        return json.loads(raw)
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


def plugin_data(response):
    if response.get("janus") != "success":
        raise AssertionError(f"plugin request failed: {response}")
    return response.get("plugindata", {}).get("data", {})


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


def write_configs(root, plugin_library, transport_library, http_port, rtp_port):
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
        '  server_name = "local-rtp-regression"\n'
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
    (config_dir / "janus.plugin.streaming.jcfg").write_text(
        "general: { events = false }\n"
        "local-rtp: {\n"
        '  type = "rtp"\n'
        "  id = 1\n"
        '  description = "Local RTP stream"\n'
        "  audio = false\n"
        "  video = true\n"
        f"  videoport = {rtp_port}\n"
        "  videopt = 96\n"
        '  videocodec = "h264"\n'
        "}\n",
        encoding="utf-8",
    )
    return config_dir


def send_rtp(port, packets=5):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
        for sequence in range(1, packets + 1):
            packet = (
                struct.pack("!BBHII", 0x80, 96, sequence, sequence * 9000, 1)
                + b"\x65\x00"
            )
            sender.sendto(packet, ("127.0.0.1", port))
            time.sleep(0.03)


def run(args):
    janus = pathlib.Path(args.janus).resolve()
    plugin = pathlib.Path(args.plugin).resolve()
    transport = pathlib.Path(args.transport).resolve()
    for required in (janus, plugin, transport):
        if not required.exists():
            raise FileNotFoundError(required)

    with tempfile.TemporaryDirectory(prefix="janus-rtp-e2e-") as temporary:
        root = pathlib.Path(temporary)
        http_port = free_port(socket.SOCK_STREAM)
        rtp_port = free_port(socket.SOCK_DGRAM)
        config_dir = write_configs(root, plugin, transport, http_port, rtp_port)
        log_path = root / "janus.log"
        with log_path.open("wb") as log_file:
            process = subprocess.Popen(
                [str(janus), "-F", str(config_dir), "-o"],
                cwd=root,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            try:
                session_id = wait_for_api(http_port, process)
                handle_id = attach_streaming(http_port, session_id)
                send_rtp(rtp_port)

                deadline = time.monotonic() + 2.0
                observed_age = None
                while time.monotonic() < deadline:
                    listed = plugin_data(
                        plugin_message(
                            http_port,
                            session_id,
                            handle_id,
                            {"request": "list"},
                            timeout=1.0,
                        )
                    )
                    mountpoints = listed.get("list", [])
                    if len(mountpoints) == 1:
                        if "rtsp_no_media_count" in mountpoints[0]:
                            raise AssertionError("RTP mountpoint exposed an RTSP-only counter")
                        media = mountpoints[0].get("media", [])
                        if len(media) == 1:
                            observed_age = media[0].get("age_ms")
                            if observed_age is not None and observed_age < 500:
                                break
                    time.sleep(0.05)
                else:
                    raise AssertionError(f"RTP media did not become fresh: age_ms={observed_age}")

                info = plugin_data(
                    plugin_message(
                        http_port,
                        session_id,
                        handle_id,
                        {"request": "info", "id": 1},
                        timeout=1.0,
                    )
                ).get("info", {})
                info_media = info.get("media", [])
                if len(info_media) != 1 or info_media[0].get("age_ms", 1000) >= 500:
                    raise AssertionError(f"info did not report fresh RTP media: {info_media}")
                if "rtsp_no_media_count" in info:
                    raise AssertionError("RTP info exposed an RTSP-only counter")

                process.send_signal(signal.SIGTERM)
                process.wait(timeout=4.0)
            except BaseException:
                log_file.flush()
                print("--- Janus RTP E2E failure log tail ---")
                print(log_path.read_text(encoding="utf-8", errors="replace")[-8000:])
                raise
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=3.0)

        log_text = log_path.read_text(encoding="utf-8", errors="replace")
        no_curl_warning = "libcurl not available, Streaming plugin will not have RTSP support"
        if args.expect_no_curl and no_curl_warning not in log_text:
            raise AssertionError("plugin was expected to be built without libcurl")
        print("RTP/UDP E2E: list/info/media/shutdown behavior verified")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--janus", required=True)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--transport", required=True)
    parser.add_argument("--expect-no-curl", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
