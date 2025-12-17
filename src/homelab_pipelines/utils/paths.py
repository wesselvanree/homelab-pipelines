from pathlib import Path


class Paths:
    repo_root = Path(__file__).parent.parent.parent.parent
    package_root = Path(__file__).parent.parent

    defs = package_root / "defs"
    defs_data = package_root / "defs" / "data"
