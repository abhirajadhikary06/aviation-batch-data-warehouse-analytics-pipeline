from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi


DEFAULT_OUTPUT_DIR = Path("data/raw")
DEFAULT_DATASET = "iamsouravbanerjee/airline-dataset"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a Kaggle dataset (or a single file) to the local raw data folder."
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help="Kaggle dataset in owner/dataset-name format.",
    )
    parser.add_argument(
        "--provider",
        choices=["kaggle", "kagglehub"],
        default="kagglehub",
        help="Download backend to use.",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Optional file name inside the dataset to download.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Destination directory for downloaded files.",
    )
    return parser.parse_args()


def configure_kaggle_env() -> None:
    """Map project env vars to Kaggle expected env vars if present."""
    load_dotenv()

    username = os.getenv("KAGGLE_USERNAME")
    key = os.getenv("KAGGLE_KEY") or os.getenv("KAGGLE_API_KEY")

    if username and key:
        os.environ["KAGGLE_USERNAME"] = username
        os.environ["KAGGLE_KEY"] = key


def authenticate_kaggle() -> KaggleApi:
    api = KaggleApi()
    api.authenticate()
    return api


def download_dataset(api: KaggleApi, dataset: str, file_name: str | None, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    if file_name:
        api.dataset_download_file(dataset=dataset, file_name=file_name, path=str(output_dir), quiet=False)
        print(f"Downloaded file '{file_name}' from '{dataset}' to '{output_dir}'.")
        return

    api.dataset_download_files(dataset=dataset, path=str(output_dir), unzip=True, quiet=False)
    print(f"Downloaded and extracted dataset '{dataset}' to '{output_dir}'.")


def download_with_kagglehub(dataset: str, output_dir: Path) -> None:
    try:
        import kagglehub
    except ImportError as exc:
        raise RuntimeError(
            "kagglehub is not installed. Install it with: pip install kagglehub"
        ) from exc

    output_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(kagglehub.dataset_download(dataset))

    for item in source_dir.iterdir():
        destination = output_dir / item.name
        if item.is_dir():
            shutil.copytree(item, destination, dirs_exist_ok=True)
        else:
            shutil.copy2(item, destination)

    print(f"Downloaded dataset '{dataset}' from kagglehub cache into '{output_dir}'.")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)

    configure_kaggle_env()

    if args.provider == "kagglehub":
        if args.file:
            raise ValueError("--file is only supported with --provider kaggle")
        download_with_kagglehub(dataset=args.dataset, output_dir=output_dir)
        return

    api = authenticate_kaggle()
    download_dataset(api=api, dataset=args.dataset, file_name=args.file, output_dir=output_dir)


if __name__ == "__main__":
    main()
