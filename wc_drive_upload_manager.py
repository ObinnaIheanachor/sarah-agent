# file: wc_drive_upload_manager.py
import io, time, threading, queue, concurrent.futures as cf
from dataclasses import dataclass
from typing import Optional, Callable
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

@dataclass
class UploadJob:
    bytes_factory: Callable[[], io.BytesIO]
    mime: str
    name: str
    parent_folder_id: str
    on_success: Optional[Callable[[str], None]] = None
    on_failure: Optional[Callable[[Exception], None]] = None
    delete_local: Optional[Callable[[], None]] = None
    size_hint: Optional[int] = None

class DriveUploadManager:
    def __init__(self, drive_client, workers: int = 20):
        self.drive = drive_client
        self.q: "queue.Queue[UploadJob]" = queue.Queue()
        self.pool = cf.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="drive-up")
        self._submitted = 0
        self._done = 0
        self._failed = 0
        self._lock = threading.Lock()
        self._stop = False
        for _ in range(workers):
            self.pool.submit(self._worker)

    def submit(self, job: UploadJob):
        with self._lock:
            self._submitted += 1
        self.q.put(job)

    def stats(self):
        with self._lock:
            return {
                "submitted": self._submitted,
                "done": self._done,
                "failed": self._failed,
                "in_queue": self.q.qsize()
            }

    def drain_and_shutdown(self, timeout: Optional[float] = None):
        while self.q.qsize() > 0:
            time.sleep(0.1)
        self._stop = True
        self.pool.shutdown(wait=True, timeout=timeout)

    @retry(stop=stop_after_attempt(6), wait=wait_exponential_jitter(0.5, 8.0))
    def _upload_once(self, job: UploadJob) -> str:
        buf = job.bytes_factory()
        file_id = self.drive.upload_stream(
            name=job.name,
            parent_id=job.parent_folder_id,
            mime=job.mime,
            stream=buf,
            size=job.size_hint
        )
        return file_id

    def _worker(self):
        while not self._stop:
            try:
                job: UploadJob = self.q.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                file_id = self._upload_once(job)
                if job.on_success: job.on_success(file_id)
                if job.delete_local: job.delete_local()
                with self._lock: self._done += 1
            except Exception as e:
                if job.on_failure: job.on_failure(e)
                with self._lock: self._failed += 1
            finally:
                self.q.task_done()
