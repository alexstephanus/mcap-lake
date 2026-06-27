FROM python:3.12-slim

WORKDIR /app

RUN pip --no-cache-dir install requests

COPY download_logs.py /app/data/download_logs.py

ENTRYPOINT ["python", "data/download_logs.py"]
# The firmware version is pinned to the 1.17.0 release by default,
# in order to keep the per-file data consistent for the Foxglove dashboard.
# The pipeline is multi-version-tolerant, so if you remove this git-hash constraint
# everything will still work, it's just that the Foxglove layout might break
# due to changes in field names
CMD ["-n", "25", "--skip-if-cached", "25", "--git-hash", "d6f12ad1c4f70ad3230afd7d86e971421e02fef4"]
