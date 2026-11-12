import argparse
import json
from pathlib import Path
from reader import inspect
from source import seed
import tempfile


def run(fixture):
    with tempfile.TemporaryDirectory() as directory:
        seed(directory, fixture)
        return inspect(directory, fixture)


def main():
    parser = argparse.ArgumentParser(description="本地来源查看")
    parser.add_argument("--fixture", type=Path, default=Path(__file__).parent / "fixtures/source.json")
    arguments = parser.parse_args()
    fixture = json.loads(arguments.fixture.read_text(encoding="utf-8"))
    print(json.dumps(run(fixture), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
