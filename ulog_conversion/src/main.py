import argparse
from dataclasses import dataclass

from convert import (
    FileConvertResult,
    _BYTES_PER_MB,
    _mbps,
    convert_filename_to_mcap,
    convert_object,
)
from storage import ObjectStore


@dataclass
class BatchConvertResult:
    n_converted: int = 0
    n_skipped: int = 0
    ulog_bytes: int = 0
    mcap_bytes: int = 0
    read_seconds: float = 0.0
    transform_seconds: float = 0.0
    write_seconds: float = 0.0

    def add_conversion(self, res: FileConvertResult):
        if res.outcome == "skipped":
            self.n_skipped += 1
        else:
            self.n_converted += 1
            self.ulog_bytes += res.ulog_bytes
            self.mcap_bytes += res.mcap_bytes
            self.read_seconds += res.read_seconds
            self.transform_seconds += res.transform_seconds
            self.write_seconds += res.write_seconds

    def log_batch(self):
        print(f"Conversion finished: {self.n_converted} files converted, {self.n_skipped} files skipped")
        if self.n_converted > 0:
            ulog_mb = self.ulog_bytes / _BYTES_PER_MB
            mcap_mb = self.mcap_bytes / _BYTES_PER_MB
            ratio = self.mcap_bytes / self.ulog_bytes
            total = self.read_seconds + self.transform_seconds + self.write_seconds
            # transform is single-threaded per file, so this MB/s is per-core capacity.
            print(
                f"transform: {_mbps(ulog_mb, self.transform_seconds):.1f} MB/s per core "
                f"({ulog_mb:.1f} MB ulg in {self.transform_seconds:.1f}s)"
            )
            print(
                f"  i/o: read {_mbps(ulog_mb, self.read_seconds):.1f} MB/s, "
                f"write {_mbps(mcap_mb, self.write_seconds):.1f} MB/s | "
                f"{ratio:.2f}x size, wall total {total:.1f}s"
            )


def batch_convert(source_bucket: str, dest_bucket: str, overwrite: bool) -> None:
    store = ObjectStore()
    batch_result = BatchConvertResult()
    source_keys = list(store.list_ulg_keys(source_bucket))
    for i, source_key in enumerate(source_keys):
        dest_key = convert_filename_to_mcap(source_key)
        file_result = convert_object(
            store, source_bucket, dest_bucket, source_key, dest_key, overwrite
        )
        batch_result.add_conversion(file_result)
        route = f"s3://{source_bucket}/{source_key} -> s3://{dest_bucket}/{dest_key}"
        file_result.log_conversion(i, len(source_keys), route)
    batch_result.log_batch()


def main():
    parser = argparse.ArgumentParser(
        description="Convert every .ulg object in --source-bucket to .mcap in --dest-bucket.",
    )
    parser.add_argument("-s", "--source-bucket", type=str, required=True)
    parser.add_argument("-d", "--dest-bucket", type=str, required=True)
    parser.add_argument("-o", "--overwrite", action="store_true")
    args = parser.parse_args()

    batch_convert(
        source_bucket=args.source_bucket,
        dest_bucket=args.dest_bucket,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()
