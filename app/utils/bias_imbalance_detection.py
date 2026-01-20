import pandas as pd
import numpy as np
import smogn

# -----------------------------
# Pandas-based random oversampling
# -----------------------------
def pandas_random_oversample(df, target_col):
    df = df.copy()
    df = df.dropna(subset=[target_col]).reset_index(drop=True)

    class_counts = df[target_col].value_counts()
    max_count = class_counts.max()

    balanced_parts = []
    for cls, count in class_counts.items():
        cls_df = df[df[target_col] == cls]
        if count < max_count:
            extra_samples = cls_df.sample(
                n=max_count - count,
                replace=True,
                random_state=42
            )
            cls_df = pd.concat([cls_df, extra_samples], ignore_index=True)
        balanced_parts.append(cls_df)

    balanced_df = pd.concat(balanced_parts, ignore_index=True)
    return balanced_df.sample(frac=1, random_state=42).reset_index(drop=True)


# -----------------------------
# Generate balanced data
# -----------------------------

def can_apply_smoter(df, target_col):
    # must be continuous
    if not np.issubdtype(df[target_col].dtype, np.number):
        return False

    # must have enough unique values
    if df[target_col].nunique() < 10:
        return False

    # must have skew / tail
    skewness = df[target_col].skew()
    if abs(skewness) < 0.5:
        return False

    return True



def generate_balanced_data(df, target_col, target_type):
    df = df.copy()

    # -----------------------------
    # 1. Drop rows where target is missing
    # -----------------------------
    df = df.dropna(subset=[target_col]).reset_index(drop=True)

    # -----------------------------
    # 2. Handle feature NaNs safely
    # -----------------------------
    for col in df.columns:
        if col == target_col:
            continue
        if pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].fillna(False)
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].astype(str).fillna("missing")

    # -----------------------------
    # 3. Balance categorical target
    # -----------------------------




    if target_type == "categorical":
        balanced_df = pandas_random_oversample(df, target_col)
        return balanced_df

    # -----------------------------
    # 4. Balance continuous target (SMOGN)
    # -----------------------------
    if target_type == "continuous":

        # 🔒 CRITICAL GUARD
        if not can_apply_smoter(df, target_col):
            print("SMOTER skipped: target has no rare/extreme regions")
            return df.copy()

        # ✅ Safe to apply SMOTER
        try:
            balanced_df = smogn.smoter(
                data=df,
                y=target_col,
                rare_value_threshold=0.05
            )
            return balanced_df

        except ValueError as e:
            print(f"SMOTER failed: {e}")
            return df.copy()

    # ---- FALLBACK ----
    return df.copy()
