# Licensing for `data/`

The contents of this folder are **not** covered by the project's MIT license
(see the repository root `LICENSE.md`). They originate from the PX4 project and
are governed by their own terms, summarized below.

## `download_logs.py`

This script is a modified copy of `download_logs.py` from the PX4
[`flight_review`](https://github.com/PX4/flight_review) project, which is
distributed under the **BSD 3-Clause License**. The local modifications (see the
docstring at the top of the file) are released under those same BSD 3-Clause
terms.

```
BSD 3-Clause License

Copyright (c) 2016-2017, PX4 Pro Drone Autopilot All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

    Neither the name of GpsDrivers nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

## Downloaded `.ulg` files (`data/raw/`)

The raw ULog (`.ulg`) flight logs are downloaded from the public PX4 flight log
database at <https://review.px4.io>. These logs are contributed by the PX4
community and are available under the [CC-BY PX4](https://creativecommons.org/licenses/by/4.0/) license.

