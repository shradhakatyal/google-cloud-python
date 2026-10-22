# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import itertools
import os
from typing import Dict, List

import yaml

try:
    from tests.perf.microbenchmarks.time_based.writes.parameters import (
        TimeBasedWriteParameters,
    )
except ModuleNotFoundError:
    from writes.parameters import TimeBasedWriteParameters

_MAX_CHUNK_SIZE_BYTES = 2 * 1024 * 1024  # 2 MiB requirement for flush_interval


def _align_flush_interval(chunk_size_bytes: int) -> int:
    """Aligns flush interval to chunk_size_bytes while enforcing 2 MiB multiples."""
    if chunk_size_bytes < _MAX_CHUNK_SIZE_BYTES:
        return _MAX_CHUNK_SIZE_BYTES
    remainder = chunk_size_bytes % _MAX_CHUNK_SIZE_BYTES
    return (
        chunk_size_bytes
        if remainder == 0
        else chunk_size_bytes + (_MAX_CHUNK_SIZE_BYTES - remainder)
    )


def _get_params() -> Dict[str, List[TimeBasedWriteParameters]]:
    """Generates benchmark parameters for time-based write operations."""
    params: Dict[str, List[TimeBasedWriteParameters]] = {}
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    common_params = config["common"]
    bucket_types = (
        [b.strip() for b in os.environ["BUCKET_TYPE"].split(",") if b.strip()]
        if "BUCKET_TYPE" in os.environ
        else common_params["bucket_types"]
    )
    file_sizes_mib = (
        [int(s.strip()) for s in os.environ["FILE_SIZE_MIB"].split(",") if s.strip()]
        if "FILE_SIZE_MIB" in os.environ
        else common_params["file_sizes_mib"]
    )
    chunk_sizes_kib = (
        [int(c.strip()) for c in os.environ["CHUNK_SIZE_KIB"].split(",") if c.strip()]
        if "CHUNK_SIZE_KIB" in os.environ
        else common_params["chunk_sizes_kib"]
    )
    rounds = (
        int(os.environ["ROUNDS"]) if "ROUNDS" in os.environ else common_params["rounds"]
    )
    duration = (
        int(os.environ["DURATION"])
        if "DURATION" in os.environ
        else common_params["duration"]
    )
    warmup_duration = (
        int(os.environ["WARMUP_DURATION"])
        if "WARMUP_DURATION" in os.environ
        else common_params["warmup_duration"]
    )

    bucket_map = {
        "rapid": os.environ.get(
            "WRITE_RAPID_BUCKET",
            os.environ.get(
                "DEFAULT_RAPID_BUCKET",
                config["defaults"]["DEFAULT_RAPID_BUCKET"],
            ),
        ),
        "regional": os.environ.get(
            "WRITE_REGIONAL_BUCKET",
            os.environ.get(
                "DEFAULT_STANDARD_BUCKET",
                config["defaults"]["DEFAULT_STANDARD_BUCKET"],
            ),
        ),
    }

    env_processes = (
        [int(p.strip()) for p in os.environ["PROCESSES"].split(",") if p.strip()]
        if "PROCESSES" in os.environ
        else None
    )
    env_coros = (
        [int(c.strip()) for c in os.environ["COROS"].split(",") if c.strip()]
        if "COROS" in os.environ
        else None
    )

    for workload in config["workload"]:
        workload_name = workload["name"]
        params[workload_name] = []
        pattern = workload["pattern"]
        processes = (
            env_processes if env_processes is not None else workload["processes"]
        )
        coros = env_coros if env_coros is not None else workload["coros"]

        product = itertools.product(
            bucket_types,
            file_sizes_mib,
            chunk_sizes_kib,
            processes,
            coros,
        )

        for (
            bucket_type,
            file_size_mib,
            chunk_size_kib,
            num_processes,
            num_coros,
        ) in product:
            file_size_bytes = file_size_mib * 1024 * 1024
            chunk_size_bytes = chunk_size_kib * 1024
            flush_interval_bytes = _align_flush_interval(
                int(os.environ["FLUSH_INTERVAL_KIB"]) * 1024
                if "FLUSH_INTERVAL_KIB" in os.environ
                else chunk_size_bytes
            )
            bucket_name = bucket_map[bucket_type]

            num_files = num_processes * num_coros

            name = (
                f"write_{pattern}_{bucket_type}_{num_processes}p_"
                f"{num_coros}c_{file_size_mib}MiB_{chunk_size_kib}KiB"
            )

            params[workload_name].append(
                TimeBasedWriteParameters(
                    name=name,
                    workload_name=workload_name,
                    pattern=pattern,
                    bucket_name=bucket_name,
                    bucket_type=bucket_type,
                    num_coros=num_coros,
                    num_processes=num_processes,
                    num_files=num_files,
                    rounds=rounds,
                    chunk_size_bytes=chunk_size_bytes,
                    file_size_bytes=file_size_bytes,
                    duration=duration,
                    warmup_duration=warmup_duration,
                    flush_interval_bytes=flush_interval_bytes,
                )
            )
    return params
