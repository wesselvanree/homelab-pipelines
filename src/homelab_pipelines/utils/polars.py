import re

import polars as pl


def to_snake_case(name: str) -> str:
    """
    Convert a string from CamelCase or PascalCase to snake_case.
    """
    # Insert underscore before uppercase letters and lowercase everything
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def rename_columns_to_snake_case(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rename all columns of a Polars DataFrame to snake_case.
    """
    new_columns = [to_snake_case(col) for col in df.columns]
    return df.rename({old: new for old, new in zip(df.columns, new_columns)})
