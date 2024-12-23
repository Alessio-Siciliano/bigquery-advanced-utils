""" Module with generic abstract class."""

from abc import ABC, abstractmethod


# Definisci una classe base astratta
class AbstractClient(ABC):
    """Abstract class for GCP classes."""

    @abstractmethod
    def _initialize(self) -> None:
        pass
