from pathlib import Path
from pydantic import BaseModel

class Settings(BaseModel):
    raw_dir: Path = Path("data/raw")
    bronze_dir: Path = Path("data/bronze")
    gold_dir: Path = Path("data/gold")

    database: str = "pipeline.duckdb"

    def setup_dirs(self):
        for d in [self.raw_dir, self.bronze_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

settings = Settings()
