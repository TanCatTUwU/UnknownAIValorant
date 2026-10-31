from __future__ import annotations

import multiprocessing
from queue import Empty, Full
import sys
import traceback


def put_latest(updates, packet):

    while True:
        try:
            updates.put_nowait(packet)
            return
        except Full:
            try:


                updates.get(timeout=.002)
            except Empty:
                pass


def engine_main(config, updates, errors, stop):

    sys.setswitchinterval(.001)
    from screen_overlay import DetectionWorker

    config = dict(config)
    render_updates = config.pop('_render_updates', None)
    worker = DetectionWorker(**config, isolated=False)
    worker._render_updates = render_updates
    worker._stop_event = stop
    status = ''
    failures = []

    def packet(detections):
        current = {
            'detections': detections,
            'status': status,
            'capture_timestamp': getattr(worker, 'capture_timestamp', None),
            'model_timestamp': getattr(worker, 'model_timestamp', None),
        }
        motion = getattr(worker, 'motion_metadata', None)
        if motion is not None:
            current['motion'] = motion
        return current

    def on_status(text):
        nonlocal status
        status = str(text)
        put_latest(updates, packet(None))

    def on_detections(detections):


        current = [(float(x1), float(y1), float(x2), float(y2), str(name), str(label))
                   for x1, y1, x2, y2, name, label in detections]
        put_latest(updates, packet(current))


    worker.status_ready.connect(on_status)
    worker.detections_ready.connect(on_detections)
    worker.failed.connect(failures.append)
    worker.run()
    if failures:
        raise RuntimeError(str(failures[-1]))


def _child_main(config, updates, errors, stop, target):


    updates.cancel_join_thread()
    try:
        target(config, updates, errors, stop)
    except BaseException:
        details = traceback.format_exc()
        try:
            errors.send(details)
        except (BrokenPipeError, EOFError, OSError):
            pass
        raise SystemExit(1)
    finally:
        errors.close()
        updates.close()


class ProcessEngine:

    def __init__(self, config, *, target=engine_main):
        context = multiprocessing.get_context('spawn')
        self._updates = context.Queue(maxsize=1)
        self._errors, self._error_sender = context.Pipe(duplex=False)
        self._stop = context.Event()
        self._render_updates = context.Queue(maxsize=1) if config.get('recordable_overlay') else None
        config = dict(config)
        if self._render_updates is not None:
            config['_render_updates'] = self._render_updates
        self._process = context.Process(
            target=_child_main,
            args=(dict(config), self._updates, self._error_sender, self._stop, target),
            name='overlay-engine',
            daemon=True,
        )
        self._started = self._closed = False
        self._failure = None
        self._exitcode = None

    @property
    def is_alive(self):
        return self._started and not self._closed and self._process.is_alive()

    @property
    def exitcode(self):
        return self._exitcode if self._closed else self._process.exitcode

    def __enter__(self):
        if self._started or self._closed:
            raise RuntimeError('An isolated engine can only be started once')
        try:
            self._process.start()
        except BaseException:
            self._close()
            raise
        self._started = True

        self._error_sender.close()
        return self

    def stop(self):
        self._stop.set()

    def send_rendered(self, regions):
        if self._closed or self._render_updates is None or self._stop.is_set():
            return False
        try:
            self._render_updates.put_nowait(regions)
            return True
        except Full:
            pass
        except (OSError, ValueError):
            return False
        try:
            self._render_updates.get_nowait()
            self._render_updates.put_nowait(regions)
        except (Empty, Full, OSError, ValueError):
            return False
        return True

    def _check_failure(self):
        if self._failure is None and self._errors is not None:
            try:
                if self._errors.poll():
                    self._failure = self._errors.recv()
            except (EOFError, BrokenPipeError, OSError):
                self._errors.close()
                self._errors = None
        if self._failure is not None:
            raise RuntimeError(str(self._failure))
        if self.exitcode is not None and not self._stop.is_set():
            raise RuntimeError(f'Isolated overlay engine exited unexpectedly (exit code {self.exitcode})')

    def poll(self, timeout=.02):
        if not self._started or self._closed:
            raise RuntimeError('The isolated engine is not running')
        self._check_failure()
        try:
            current = self._updates.get(timeout=max(0., float(timeout)))
        except Empty:
            current = None

        for _ in range(8):
            try:
                current = self._updates.get_nowait()
            except Empty:
                break
        self._check_failure()
        return current

    def _close(self):
        self._exitcode = self._process.exitcode
        self._updates.close()
        self._updates.cancel_join_thread()
        if self._render_updates is not None:
            self._render_updates.close()
            self._render_updates.cancel_join_thread()
        if self._errors is not None:
            self._errors.close()
            self._errors = None
        self._error_sender.close()
        self._process.close()
        self._closed = True

    def __exit__(self, *args):
        self.stop()
        if self._started:
            self._process.join(timeout=3)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=1)
        self._close()
