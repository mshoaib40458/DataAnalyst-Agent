import pandas as pd
from typing import Dict, Any


def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Profiles a pandas dataframe and returns a dictionary with summary statistics.
    """
    profile = {
        "num_rows": int(len(df)),
        "num_cols": int(len(df.columns)),
        "numeric_columns": [],
        "categorical_columns": [],
        "datetime_columns": [],
        "columns": {}
    }

    for col in df.columns:
        col_type = str(df[col].dtype)
        col_profile = {
            "type": col_type,
            "num_missing": int(df[col].isna().sum()),
            "num_unique": int(df[col].nunique())
        }

        if pd.api.types.is_numeric_dtype(df[col]):
            profile["numeric_columns"].append(col)
            col_profile["mean"] = float(df[col].mean()) if not pd.isna(df[col].mean()) else None
            col_profile["median"] = float(df[col].median()) if not pd.isna(df[col].median()) else None
            col_profile["variance"] = float(df[col].var()) if not pd.isna(df[col].var()) else None
            col_profile["min"] = float(df[col].min()) if not pd.isna(df[col].min()) else None
            col_profile["max"] = float(df[col].max()) if not pd.isna(df[col].max()) else None

        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            profile["datetime_columns"].append(col)
            col_profile["min"] = str(df[col].min()) if not pd.isna(df[col].min()) else None
            col_profile["max"] = str(df[col].max()) if not pd.isna(df[col].max()) else None

        elif pd.api.types.is_object_dtype(df[col]):
            parsed = pd.to_datetime(df[col], errors="coerce")
            datetime_ratio = float(parsed.notna().mean())
            if datetime_ratio >= 0.8:
                col_profile["detected_as"] = "datetime_like"
                profile["datetime_columns"].append(col)
                col_profile["min"] = str(parsed.min()) if not pd.isna(parsed.min()) else None
                col_profile["max"] = str(parsed.max()) if not pd.isna(parsed.max()) else None
            else:
                profile["categorical_columns"].append(col)
                value_counts = df[col].value_counts().head(5).to_dict()
                col_profile["top_values"] = {str(k): int(v) for k, v in value_counts.items()}

        elif isinstance(df[col].dtype, pd.CategoricalDtype):
            profile["categorical_columns"].append(col)
            value_counts = df[col].value_counts().head(5).to_dict()
            col_profile["top_values"] = {str(k): int(v) for k, v in value_counts.items()}

        profile["columns"][col] = col_profile

    return profile
