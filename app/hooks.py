"""回归测试用的时序控制钩子。

仅当环境变量 ``COLLECTOR_TEST_HOOKS=1`` 时启用；默认情况下 :meth:`Hooks.gate`
与 :meth:`Hooks.wait` 都是立即返回的空操作，生产环境行为不受任何影响。

设计为一组命名“闸门”：
- 业务代码在关键点 ``await hooks.gate("name")``：先把 ``reached:name`` 置位，
  再阻塞等待测试侧 ``hooks.set("name")`` 放行；
- 测试侧 ``await hooks.wait("name")`` 等待代码执行到该点，然后 ``hooks.set("name")``
  放行，从而确定性地控制“写入中 / 等锁中 / 取消时 / 并发查询”的交错时序。

事件对象在运行中的事件循环内惰性创建，保证与 ASGI 请求任务处于同一循环。
"""
from __future__ import annotations

import asyncio

from . import config


class Hooks:
    def __init__(self) -> None:
        self._events: dict[str, asyncio.Event] = {}
        self._armed: set[str] = set()

    @property
    def enabled(self) -> bool:
        return config.TEST_HOOKS_ENABLED

    def reset(self) -> None:
        """清空所有闸门（每个测试用例开始时调用）。"""
        self._events.clear()
        self._armed.clear()

    def arm(self, name: str) -> None:
        """测试侧武装某个闸门：武装后业务代码到达该点才会阻塞等待。"""
        self._armed.add(name)

    def _event(self, name: str) -> asyncio.Event:
        event = self._events.get(name)
        if event is None:
            event = asyncio.Event()
            self._events[name] = event
        return event

    def set(self, name: str) -> None:
        """测试侧放行某个闸门。"""
        self._event(f"allow:{name}").set()

    def is_set(self, name: str) -> bool:
        return self._events.get(f"reached:{name}") is not None and self._events[
            f"reached:{name}"
        ].is_set()

    async def wait(self, name: str) -> None:
        """测试侧等待业务代码到达某个闸门（隐式武装，保证后续到达会阻塞）。"""
        if not self.enabled:
            return
        self._armed.add(name)
        await self._event(f"reached:{name}").wait()

    async def gate(self, name: str) -> None:
        """业务代码调用。

        仅当测试侧武装（:meth:`arm` / :meth:`wait`）了同名闸门时才阻塞：
        未武装的用例（如并发提交压测）完全不受钩子影响，事务照常快速提交，
        不会出现“拿着写锁等钩子”导致的锁竞争。
        """
        if not self.enabled or name not in self._armed:
            return
        self._event(f"reached:{name}").set()
        await self._event(f"allow:{name}").wait()


hooks = Hooks()
