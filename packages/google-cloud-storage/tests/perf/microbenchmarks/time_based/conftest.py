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
import uuid

import pytest

from tests.perf.microbenchmarks.time_based.writes.parameters import (
    TimeBasedWriteParameters,
)


@pytest.fixture
def workload_params(request):
    params = request.param
    if isinstance(params, TimeBasedWriteParameters):
        run_id = uuid.uuid4().hex[:8]
        files_names = [
            f"micro-benchmark-write-{run_id}.{i}" for i in range(0, params.num_files)
        ]
    else:
        files_names = [f"fio-go_storage_fio.0.{i}" for i in range(0, params.num_files)]
    return params, files_names
