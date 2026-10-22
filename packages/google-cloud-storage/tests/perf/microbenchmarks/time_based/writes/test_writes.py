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
"""Microbenchmarks for time-based Google Cloud Storage write operations."""

import asyncio
import logging
import multiprocessing
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, NamedTuple, Optional

import pytest

import tests.perf.microbenchmarks.time_based.writes.config as config
from google.cloud.storage._helpers import _parse_bool_env
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)
from google.cloud.storage.asyncio.async_grpc_client import AsyncGrpcClient
from tests.perf.microbenchmarks._utils import (
    get_irq_affinity,
    publish_benchmark_extra_info,
)
from tests.perf.microbenchmarks.conftest import (
    publish_resource_metrics,
)

all_params = config._get_params()
RCU_SYSTEM_TESTS = _parse_bool_env("RUN_RCU_SYSTEM_TESTS", default=False)


class UploadResult(NamedTuple):
    total_bytes: int
    measured_start_time: float
    measured_end_time: float


async def create_client():
    """Initializes async client and gets the current event loop."""
    return AsyncGrpcClient()


def _aggregate_upload_results(results: List[UploadResult]) -> UploadResult:
    if not results:
        raise ValueError("At least one upload result is required.")

    total_bytes = sum(result.total_bytes for result in results)
    measured_start_time = min(result.measured_start_time for result in results)
    measured_end_time = max(result.measured_end_time for result in results)
    if measured_end_time <= measured_start_time:
        raise ValueError("Measured elapsed time must be positive.")

    return UploadResult(
        total_bytes=total_bytes,
        measured_start_time=measured_start_time,
        measured_end_time=measured_end_time,
    )


def _calculate_average_throughput_mib_s(
    upload_bytes_list: List[int], upload_elapsed_times: List[float]
) -> float:
    total_bytes_uploaded = sum(upload_bytes_list)
    total_elapsed_time = sum(upload_elapsed_times)
    if total_elapsed_time <= 0:
        raise ValueError("Total measured elapsed time must be positive.")

    return (total_bytes_uploaded / total_elapsed_time) / (1024 * 1024)


def _build_upload_result(
    total_bytes_uploaded: int,
    measured_start_time: Optional[float],
    measured_end_time: Optional[float],
) -> UploadResult:
    if measured_start_time is None or measured_end_time is None:
        raise ValueError("No uploads completed during the measured interval.")

    return UploadResult(
        total_bytes=total_bytes_uploaded,
        measured_start_time=measured_start_time,
        measured_end_time=measured_end_time,
    )


# --- Global Variables for Worker Process ---
worker_loop = None
worker_client = None
worker_json_client = None


def _worker_init(bucket_type):
    """Initializes a persistent event loop and client for each worker process."""
    cpu_affinity = get_irq_affinity()
    if cpu_affinity:
        os.sched_setaffinity(
            0, {i for i in range(0, os.cpu_count()) if i not in cpu_affinity}
        )

    global worker_loop, worker_client, worker_json_client
    if bucket_type == "rapid":
        worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(worker_loop)
        worker_client = worker_loop.run_until_complete(create_client())
    else:  # regional
        from google.cloud import storage

        worker_json_client = storage.Client()


def _upload_time_based_json_single(client, filename, chunk_data, params):
    """Performs time-based uploads for a single worker thread using the JSON API."""
    total_bytes_uploaded = 0
    bucket = client.bucket(params.bucket_name)
    blob = bucket.blob(filename)
    blob.chunk_size = params.chunk_size_bytes

    is_warming_up = True
    start_time = time.perf_counter()
    warmup_end_time = start_time + params.warmup_duration
    test_end_time = warmup_end_time + params.duration
    measured_start_time = None
    measured_end_time = None

    while time.perf_counter() < test_end_time or measured_end_time is None:
        current_time = time.perf_counter()
        if is_warming_up and current_time >= warmup_end_time:
            is_warming_up = False
            total_bytes_uploaded = 0
            measured_start_time = current_time

        blob.upload_from_string(chunk_data)

        if not is_warming_up:
            total_bytes_uploaded += len(chunk_data)
            measured_end_time = time.perf_counter()

    return _build_upload_result(
        total_bytes_uploaded, measured_start_time, measured_end_time
    )


def _upload_time_based_json(client, filenames, params):
    """Performs time-based uploads across one or more threads using the JSON API."""
    chunk_data = os.urandom(params.chunk_size_bytes)
    if len(filenames) == 1:
        res = _upload_time_based_json_single(client, filenames[0], chunk_data, params)
        return _aggregate_upload_results([res])

    with ThreadPoolExecutor(max_workers=len(filenames)) as executor:
        futures = [
            executor.submit(
                _upload_time_based_json_single, client, f, chunk_data, params
            )
            for f in filenames
        ]
        results = [future.result() for future in futures]
    return _aggregate_upload_results(results)


async def _upload_time_based_async(client, filenames, params):
    """Performs time-based uploads using AsyncAppendableObjectWriter."""
    chunk_data = os.urandom(params.chunk_size_bytes)
    # Cap per-object size at 2 GiB (or file_size_bytes if smaller) and overwrite
    # in place so 48 concurrent streams never exceed ~96 GiB of active Rapid
    # zonal storage quota during long multi-round runs.
    max_object_bytes = min(params.file_size_bytes, 2 * 1024 * 1024 * 1024)

    async def _worker_coro(filename):
        writer = None
        bytes_in_current_object = 0
        total_bytes_uploaded = 0
        is_warming_up = True
        start_time = time.perf_counter()
        warmup_end_time = start_time + params.warmup_duration
        test_end_time = warmup_end_time + params.duration
        measured_start_time = None
        measured_end_time = None

        try:
            while time.perf_counter() < test_end_time or measured_end_time is None:
                if writer is None or bytes_in_current_object >= max_object_bytes:
                    if writer is not None:
                        await writer.close(finalize_on_close=True)
                    writer = AsyncAppendableObjectWriter(
                        client=client,
                        bucket_name=params.bucket_name,
                        object_name=filename,
                        writer_options={
                            "FLUSH_INTERVAL_BYTES": params.flush_interval_bytes
                        },
                        storage_class="RAPID" if RCU_SYSTEM_TESTS else None,
                    )
                    await writer.open()
                    bytes_in_current_object = 0

                current_time = time.perf_counter()
                if is_warming_up and current_time >= warmup_end_time:
                    is_warming_up = False
                    total_bytes_uploaded = 0
                    measured_start_time = current_time

                await writer.append(chunk_data)
                bytes_in_current_object += params.chunk_size_bytes

                if not is_warming_up:
                    total_bytes_uploaded += params.chunk_size_bytes
                    measured_end_time = time.perf_counter()

            if writer is not None and writer.is_stream_open:
                await writer.close(finalize_on_close=True)
                measured_end_time = time.perf_counter()
        finally:
            if writer is not None and writer.is_stream_open:
                await writer.close(finalize_on_close=True)

        return _build_upload_result(
            total_bytes_uploaded, measured_start_time, measured_end_time
        )

    tasks = [asyncio.create_task(_worker_coro(f)) for f in filenames]
    results = await asyncio.gather(*tasks)
    return _aggregate_upload_results(results)


def _upload_files_worker(process_idx, filenames, params, bucket_type):
    if bucket_type == "rapid":
        return worker_loop.run_until_complete(
            _upload_time_based_async(worker_client, filenames, params)
        )
    else:  # regional
        return _upload_time_based_json(worker_json_client, filenames, params)


def upload_files_mp_mc_wrapper(pool, files_names, params, bucket_type):
    num_coros = params.num_coros
    filenames_per_process = [
        files_names[i : i + num_coros] for i in range(0, len(files_names), num_coros)
    ]
    args = [
        (i, filenames_per_process[i], params, bucket_type)
        for i in range(len(filenames_per_process))
    ]

    results = pool.starmap(_upload_files_worker, args)
    agg_res = _aggregate_upload_results(results)
    return agg_res.total_bytes, agg_res.measured_end_time - agg_res.measured_start_time


@pytest.mark.parametrize(
    "workload_params",
    all_params["write_seq_multi_process"],
    indirect=True,
    ids=lambda p: p.name,
)
def test_uploads_multi_proc_multi_coro(
    benchmark, storage_client, blobs_to_delete, monitor, workload_params
):
    params, files_names = workload_params
    logging.info(f"num files: {len(files_names)}")

    ctx = multiprocessing.get_context("spawn")
    pool = ctx.Pool(
        processes=params.num_processes,
        initializer=_worker_init,
        initargs=(params.bucket_type,),
    )

    upload_bytes_list = []
    upload_elapsed_times = []

    def target_wrapper(*args, **kwargs):
        total_bytes, measured_elapsed_time = upload_files_mp_mc_wrapper(
            pool, *args, **kwargs
        )
        upload_bytes_list.append(total_bytes)
        upload_elapsed_times.append(measured_elapsed_time)
        return

    try:
        with monitor() as m:
            benchmark.pedantic(
                target=target_wrapper,
                iterations=1,
                rounds=params.rounds,
                args=(files_names, params, params.bucket_type),
            )
    finally:
        pool.close()
        pool.join()
        blobs_to_delete.extend(
            storage_client.bucket(params.bucket_name).blob(f) for f in files_names
        )

    throughput_mib_s = _calculate_average_throughput_mib_s(
        upload_bytes_list, upload_elapsed_times
    )
    benchmark.extra_info["avg_throughput_mib_s"] = f"{throughput_mib_s:.2f}"
    print(f"Avg Throughput of {params.rounds} round(s): {throughput_mib_s:.2f} MiB/s")
    publish_benchmark_extra_info(
        benchmark,
        params,
        benchmark_group="write",
        download_bytes_list=upload_bytes_list,
        duration=upload_elapsed_times,
    )
    publish_resource_metrics(benchmark, m)
