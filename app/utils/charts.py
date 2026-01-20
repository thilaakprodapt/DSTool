import matplotlib
matplotlib.use('Agg')

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as stats
from itertools import combinations
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import io
import os
from datetime import datetime
# from BQconnection import find_target_column

sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams["figure.autolayout"] = True

# ======================================================
# CONFIG
# ======================================================
# SERVICE_ACCOUNT_JSON = './cloud-practice-dev-2-edda3798bdf3.json'
BQ_PROJECT = 'cloud-practice-dev-2'
BQ_DATASET = 'EDADataset'
BQ_TABLE = 'Books'
GCP_BUCKET = 'cloudassist1'
GCP_CHARTS_FOLDER = 'charts-ds'

# TARGET_COLUMN = find_target_column(BQ_DATASET,BQ_TABLE)      # <<<  ADD YOUR TARGET COLUMN HERE



# Use configured credentials from config.py
from app.core.config import PROJECT_ID, gcs_client, bq_client

storage_client = gcs_client
bq_read_client = bq_client
 

# ======================================================
# UPLOAD FIG TO GCS
# ======================================================
def upload_fig_to_gcs(fig, name, expiration_hours=24):
    """
    Upload a matplotlib figure to GCS and return a signed URL.
    
    Args:
        fig: matplotlib figure object
        name: base name for the file
        expiration_hours: how long the signed URL should be valid (default: 24 hours)
    
    Returns:
        str: Signed URL that provides temporary access to the image
    """
    from datetime import timedelta
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{ts}.png"

    bucket = storage_client.bucket(GCP_BUCKET)
    blob = bucket.blob(f"{GCP_CHARTS_FOLDER}/{filename}")

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)

    blob.upload_from_string(buf.getvalue(), content_type='image/png')
    buf.close()

    # Generate signed URL (valid for specified hours)
    signed_url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=expiration_hours),
        method="GET"
    )
    
    print(f"✓ Uploaded: gs://{GCP_BUCKET}/{GCP_CHARTS_FOLDER}/{filename}")
    print(f"✓ Signed URL (valid for {expiration_hours}h): {signed_url[:80]}...")
    return signed_url



# ======================================================
# UNIVARIATE – NUMERICAL
# ======================================================
def univariate_numerical(df, num_cols):
    if not num_cols:
        return None

    cols = 2
    rows = (len(num_cols) + 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(14, 4 * rows))
    axes = axes.flatten()

    for i, col in enumerate(num_cols):
        sns.histplot(df[col], bins=30, kde=True, ax=axes[i])
        axes[i].set_title(col)

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    plt.suptitle("Histogram Grid (Numerical)", fontsize=16)
    url = upload_fig_to_gcs(fig, "univariate_numerical")
    plt.close(fig)
    return url



# ======================================================
# UNIVARIATE – CATEGORICAL
# ======================================================
def univariate_categorical(df, cat_cols):
    if not cat_cols:
        return None

    cols = 2
    rows = (len(cat_cols) + 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(14, 4 * rows))
    axes = axes.flatten()

    for i, col in enumerate(cat_cols):
        clean_series = df[col].astype(str).str.strip().replace("", np.nan).dropna()

        if clean_series.empty:
            axes[i].text(0.5, 0.5, f"No valid data\nin {col}",
                         ha='center', va='center', fontsize=12)
            axes[i].set_title(col)
            axes[i].set_axis_off()
            continue

        top_values = clean_series.value_counts().head(10).index
        sns.countplot(x=clean_series, order=top_values, ax=axes[i])
        axes[i].set_title(col)
        axes[i].tick_params(axis='x', rotation=45)

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    plt.suptitle("Bar Chart Grid (Categorical)", fontsize=16)
    url = upload_fig_to_gcs(fig, "univariate_categorical")
    plt.close(fig)
    return url



# ======================================================
# TARGET-BASED BIVARIATE — NUMERIC TARGET
# ======================================================
def numeric_target_analysis(df, target, num_cols, cat_cols):
    results = {}

    # -----------------------------
    # 1. Correlation Bar Chart
    # -----------------------------
    numeric_features = [c for c in num_cols if c != target]
    corr = df[numeric_features].corrwith(df[target]).sort_values()

    fig, ax = plt.subplots(figsize=(10, 7))
    corr.plot(kind='bar', ax=ax)
    ax.set_title(f"Correlation with Target: {target}")
    ax.set_ylabel("Correlation")

    results["Correlation with Target (Bar Chart)"] = upload_fig_to_gcs(
        fig, "target_numeric_correlation"
    )
    plt.close(fig)

    # -----------------------------
    # 2. Categorical vs Target – Mean Plot Grid
    # -----------------------------
    if cat_cols:
        cols = 2
        rows = (len(cat_cols) + 1) // cols

        fig, axes = plt.subplots(rows, cols, figsize=(14, 4 * rows))
        axes = axes.flatten()

        for i, col in enumerate(cat_cols):
            if df[col].nunique() > 20:
                df[col] = df[col].astype(str)

            sns.barplot(x=col, y=target, data=df, ax=axes[i], estimator=np.mean)
            axes[i].set_title(f"{col} vs {target} (Mean)")
            axes[i].tick_params(axis='x', rotation=45)

        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        results["Categorical vs Target (Mean Plot Grid)"] = upload_fig_to_gcs(
            fig, "target_cat_mean_grid"
        )
        plt.close(fig)

    return results



# ======================================================
# TARGET-BASED BIVARIATE — CATEGORICAL TARGET
# ======================================================
def categorical_target_analysis(df, target, num_cols, cat_cols):
    results = {}

    # -----------------------------
    # 1. Numerical vs Target → Boxplot Grid
    # -----------------------------
    if num_cols:
        cols = 2
        rows = (len(num_cols) + 1) // cols

        fig, axes = plt.subplots(rows, cols, figsize=(14, 4 * rows))
        axes = axes.flatten()

        for i, num in enumerate(num_cols):
            sns.boxplot(x=df[target], y=df[num], ax=axes[i])
            axes[i].set_title(f"{num} vs {target}")
            axes[i].tick_params(axis='x', rotation=25)

        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        results["Numerical vs Target (Boxplot Grid)"] = upload_fig_to_gcs(
            fig, "target_boxplot_grid"
        )
        plt.close(fig)

    # -----------------------------
    # 2. Categorical vs Target → Chi-Square Importance Bar Chart
    # -----------------------------
    scores = {}

    for col in cat_cols:
        try:
            confusion = pd.crosstab(df[col], df[target])
            chi2 = stats.chi2_contingency(confusion)[0]
            scores[col] = chi2
        except:
            scores[col] = 0

    scores = pd.Series(scores).sort_values()

    fig, ax = plt.subplots(figsize=(10, 7))
    scores.plot(kind="barh", ax=ax)
    ax.set_title(f"Categorical Importance (Chi-square with {target})")
    ax.set_xlabel("Chi-square Score")

    results["Categorical Importance (Chi-square Bar Chart)"] = upload_fig_to_gcs(
        fig, "target_chi_square_importance"
    )
    plt.close(fig)

    return results



# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":

    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    df = bq_client.list_rows(bq_client.get_table(table_ref)).to_dataframe()

    num_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    results = {}

    # ---- Univariate ----
    results["Histogram grid (numerical)"] = univariate_numerical(df, num_cols)
    results["Bar chart grid (categorical)"] = univariate_categorical(df, cat_cols)

    # ---- Target-based Bivariate ----
    if TARGET_COLUMN in num_cols:
        print(f"\n>>> Target is NUMERICAL → Running numerical-target analysis")
        results.update(numeric_target_analysis(df, TARGET_COLUMN, num_cols, cat_cols))

    elif TARGET_COLUMN in cat_cols:
        print(f"\n>>> Target is CATEGORICAL → Running categorical-target analysis")
        results.update(categorical_target_analysis(df, TARGET_COLUMN, num_cols, cat_cols))

    else:
        print(f"!!! ERROR: Target column {TARGET_COLUMN} not found in dataset")

    print("\n=== Uploaded Chart URLs ===")
    for k, v in results.items():
        print(f"{k}: {v}")

