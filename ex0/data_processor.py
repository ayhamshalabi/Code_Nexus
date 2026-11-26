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
        if not isinstance(data, dict) or not data:
            return False
        if not all(
            isinstance(k, str) and isinstance(v, str) for k, v in data.items()
        ):
            return False
        return "log_level" in data and "log_message" in data

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
            formatted: str = f"{item["log_level"]}: {item["log_message"]}"
            self._data.append((self._rank, formatted))
            self._rank += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    num_proc = NumericProcessor()
    print(f"Trying to validate input '42': {num_proc.validate(42)}")
    print(f"Trying to validate input 'Hello': {num_proc.validate("Hello")}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:    def _is_valid_dict(self, data: Any) -> bool:
        return isinstance(data, dict)

        num_proc.ingest("foo")  # type: ignore
    except Exception as err:
        print(f"Got exception: {err}")
    num_data: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_data}")
    num_proc.ingest(num_data)
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = num_proc.output()
        print(f"Numeric value {rank}: {val}")

    print("\nTesting Text Processor...")
    text_proc = TextProcessor()
    print(f"Trying to validate input '42': {text_proc.validate(42)}")
    text_data: list[str] = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {text_data}")
    text_proc.ingest(text_data)
    print("Extracting 1 value...")
    rank, val = text_proc.output()
    print(f"Text value {rank}: {val}")

    print("\nTesting Log Processor...")
    log_proc = LogProcessor()
    print(f"Trying to validate input 'Hello': {log_proc.validate("Hello")}")
    log_data: list[dict[str, str]] = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {log_data}")
    log_proc.ingest(log_data)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = log_proc.output()
        print(f"Log entry {rank}: {val}")
