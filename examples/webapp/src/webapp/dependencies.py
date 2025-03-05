from that_depends import BaseContainer
from that_depends.providers import Singleton

from webapp.settings import Settings


class Dependencies(BaseContainer):
    """Dependency injection container."""

    settings: Singleton[Settings] = Singleton(Settings)  # pyright: ignore[reportCallIssue]
