from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from pydantic import BaseModel, field_validator

# Load .env if present
load_dotenv()


class DataFolder(BaseModel):
    proj_root: Path
    data_dir: Path
    raw: Path
    interim: Path
    processed: Path
    external: Path
    models_dir: Path
    reports_dir: Path
    figures_dir: Path

    @field_validator("proj_root", mode="before")
    @classmethod
    def resolve_proj_root(cls, v: str | Path) -> Path:
        return Path(v).resolve()

    @classmethod
    def from_proj_root(cls, proj_root: Path | None = None) -> "DataFolder":
        proj_root = proj_root or Path(__file__).resolve().parents[1]
        logger.info(f"Using PROJ_ROOT: {proj_root}")

        return cls(
            proj_root=proj_root,
            data_dir=proj_root / "data",
            raw=proj_root / "data" / "raw",
            interim=proj_root / "data" / "interim",
            processed=proj_root / "data" / "processed",
            external=proj_root / "data" / "external",
            models_dir=proj_root / "models",
            reports_dir=proj_root / "reports",
            figures_dir=proj_root / "reports" / "figures",
        )

