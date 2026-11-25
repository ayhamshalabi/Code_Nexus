from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise ValueError("No data available to output")
        return self._data.pop(0)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def _is_valid_number(self, data: Any) -> bool:
        return isinstance(data, (int, float)) and not isinstance(data, bool)

    def validate(self, data: Any) -> bool:
        if self._is_valid_number(data):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(self._is_valid_number(item) for item in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        items: list[int | float] = data if isinstance(data, list) else [data]

        for item in items:
            self._data.append((self._rank, str(item)))
            self._rank += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def _is_valid_string(self, data: Any) -> bool:
        return isinstance(data, str)

    def validate(self, data: Any) -> bool:
        if self._is_valid_string(data):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(self._is_valid_string(item) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        items: list[str] = data if isinstance(data, list) else [data]

        for item in items:
            self._data.append((self._rank, item))
            self._rank += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def _is_valid_dict(self, data: Any) -> bool:
        return isinstance(data, dict)

    def validate(self, data: Any) -> bool:
        if self._is_valid_dict(data):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(self._is_valid_dict(item) for item in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper dict data")

        items: list[dict[str, str]] = (
            data if isinstance(data, list) else [data]
        )

        for item in items:
            formatted: str = f""
            self._data.append(self._rank, stformatted)
            self._rank += 1


def main():
    pass


if __name__ == "__main__":
    main()
