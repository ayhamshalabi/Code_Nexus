from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self, name: str) -> None:
        self._name: str = name
        self._data: list[tuple[int, str]] = []
        self._rank: int = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def total(self) -> int:
        return self._rank

    @property
    def remaining(self) -> int:
        return len(self._data)

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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__(name="Numeric Processor")

    def _is_valid_number(self, data: Any) -> bool:
        return not isinstance(data, bool) and isinstance(data, (int, float))

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
        super().__init__(name="Text Processor")

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
        super().__init__(name="Log Processor")

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


class DataStream:
    def __init__(self) -> None:
        super().__init__()
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        if not isinstance(proc, DataProcessor):
            raise TypeError("Processor must be an instance of DataProcessor")
        if proc in self._processors:
            raise ValueError("Processor is already registered in this stream")
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            handled: bool = False
            for proc in self._processors:
                if proc.validate(element):
                    try:
                        proc.ingest(element)
                        handled = True
                        break
                    except Exception as err:
                        print(f"DataStream error - {err}")
            if not handled:
                print(
                    f"DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(
                f"{proc.name}: "
                f"total {proc.total} items processed, "
                f"remaining {proc.remaining} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            extracted: list[tuple[int, str]] = []
            for _ in range(nb):
                if proc.remaining == 0:
                    break
                extracted.append(proc.output())
            if extracted:
                plugin.process_output(extracted)


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        csv_line: str = ",".join(value for _, value in data)
        print("CSV Output:")
        print(csv_line)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        pairs: list[str] = []
        for rank, value in data:
            escaped_val: str = value.replace('"', '\\"')
            pairs.append(f'"item_{rank}": "{escaped_val}"')
        json_str: str = "{" + ", ".join(pairs) + "}"
        print("JSON Output:")
        print(json_str)


def main() -> None:
    print("=== Code Nexus Data Pipeline ===")
    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    batch1: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"\nSend first batch of data on stream: {batch1}")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    stream.output_pipeline(3, csv_plugin)
    stream.print_processors_stats()

    batch2: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]

    print(f"\nSend another batch of data:\n{batch2}")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    stream.output_pipeline(5, json_plugin)
    stream.print_processors_stats()


if __name__ == "__main__":
    main()