"""Python version compatibility helpers."""

import asyncio
import contextlib
import sys
from collections.abc import AsyncGenerator

if sys.version_info >= (3, 11):
    from builtins import ExceptionGroup
    from typing import Self
else:
    from exceptiongroup import ExceptionGroup
    from typing_extensions import Self

__all__ = ("ExceptionGroup", "Self", "defer_cancellation")

if sys.version_info >= (3, 11):

    @contextlib.asynccontextmanager
    async def defer_cancellation() -> AsyncGenerator[None, None]:
        """Temporarily remove pending cancellations, restore them after cleanup."""
        task = asyncio.current_task()
        cancels = task.cancelling() if task else 0
        for _ in range(cancels):
            if task:
                task.uncancel()
        try:
            yield
        finally:
            for _ in range(cancels):
                if task:
                    task.cancel()

else:

    @contextlib.asynccontextmanager
    async def defer_cancellation() -> AsyncGenerator[None, None]:
        """No-op on Python < 3.11: cancelling()/uncancel() are not available."""
        yield
