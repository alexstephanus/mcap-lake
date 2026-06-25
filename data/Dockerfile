FROM python:3.12-slim

WORKDIR /app

RUN pip --no-cache-dir install requests

COPY download_logs.py /app/data/download_logs.py

ENTRYPOINT ["python", "data/download_logs.py"]
CMD ["-n", "25", "--skip-if-cached", "25"]
