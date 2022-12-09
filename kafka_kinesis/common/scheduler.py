import time
from typing import Callable, Optional
import asyncio
import functools
import inspect
from datetime import datetime, timedelta
from kafka_kinesis.config import config

logger = config.logger


class AIOScheduler:
    """Async Scheduler"""

    def __init__(
        self,
        freq: timedelta,
        func: Optional[Callable] = None,
        args: tuple = (),
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.freq = freq
        self.func = func
        self.args = args
        self.cron = self.wrap_f(self.func)
        self.handle: Optional[asyncio.Handle] = None
        self.loop = loop if loop else asyncio.get_event_loop()
        self.time: datetime = datetime.now()

    @staticmethod
    def wrap_f(func: Callable):
        """wraps an async function and waits for completion"""

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            return result

        return wrapper

    def start(self):
        """Start the scheduler"""
        logger.info("Starting scheduler")
        self.stop()
        self.handle = self.loop.call_soon_threadsafe(self.call_next, *self.args)

    def stop(self):
        """Stop the scheduler"""
        logger.info("Stop and reset")
        if self.handle:
            self.handle.cancel()
        self.handle = None

    def get_next(self):
        """Get next execution"""
        return ((self.time + self.freq) - self.time).seconds

    def call_next(self, *args, **kwargs):
        """Recursively reconstruct and call the function"""
        if self.handle is not None:
            self.handle.cancel()
        self.handle = self.loop.call_later(self.get_next(), self.call_next, *self.args)
        logger.info(f"Calling cron func at:\n Current Time: {time.time()}")
        asyncio.gather(
            self.cron(*args, **kwargs), loop=self.loop, return_exceptions=True
        ).add_done_callback(self.on_done)
        # update time
        self.time = datetime.now()

    @staticmethod
    def on_done(result):
        """Callback when future is completed"""
        result = result.result()[0]
        logger.info(f"Received result: {result}")
        if isinstance(result, Exception):
            logger.error(f"Error encountered: {result}")
            raise result

    def __call__(self, func):
        """Call object directly with function"""
        self.func = func
        self.cron = self.wrap_f(func)
        self.loop.call_soon_threadsafe(self.start)
        return self

    def __str__(self) -> str:
        return f"{str(self.freq)} {self.func}"

    def __repr__(self):
        return f"<AIOScheduler {str(self.freq)} {self.func}"
