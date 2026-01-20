import pandas as pd
import numpy as np
from google.cloud import bigquery
from scipy.stats import chi2_contingency


# Initialize BigQuery client (pass this from your main app)
bq_client = None

def set_bq_client(client):
    """Set the BigQuery client for this module"""
    global bq_client
    bq_client = client


# ======================================================
# DATA OVERVIEW (FAST - Uses BigQuery metadata)
# ======================================================
def compute_data_overview(bq_client, project, dataset, table, df_sample):
    """
    Compute basic data overview using BigQuery metadata
    """
    # Get total row count from BigQuery (instant)
    table_ref = bq_client.get_table(f"{project}.{dataset}.{table}")
    total_rows = table_ref.num_rows
    
    # Convert sample for JSON serialization
    df_copy = df_sample.head(3).copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(str)
        elif pd.api.types.is_timedelta64_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(str)
    
    sample_data = df_copy.to_dict(orient="records")
    
    return {
        "rows": int(total_rows),
        "columns": len(df_sample.columns),
        "sample_data": sample_data
    }


# ======================================================
# MISSING VALUES (BQ-OPTIMIZED)
# ======================================================
def compute_missing_values_bq(bq_client, project, dataset, table):
    """
    Compute missing values using BigQuery (MUCH FASTER for large datasets)
    """
    query = f"""
    SELECT
        STRUCT<column_name STRING, missing_count INT64, missing_pct FLOAT64> AS stats
    FROM (
        SELECT 'column_placeholder' as column_name, 0 as missing_count, 0.0 as missing_pct
    )
    LIMIT 0
    """
    
    # Get schema to identify all columns
    table_ref = bq_client.get_table(f"{project}.{dataset}.{table}")
    schema = table_ref.schema
    
    # Build dynamic query for all columns
    column_checks = []
    for field in schema:
        col_name = field.name
        column_checks.append(f"COUNTIF({col_name} IS NULL) as {col_name}_missing")
    
    missing_query = f"""
    SELECT
        {', '.join(column_checks)},
        COUNT(*) as total_rows
    FROM `{project}.{dataset}.{table}`
    """
    
    result = bq_client.query(missing_query).to_dataframe()
    total = result['total_rows'].iloc[0]
    
    missing_dict = {}
    missing_pct_dict = {}
    
    for field in schema:
        col_name = field.name
        missing_col = f"{col_name}_missing"
        if missing_col in result.columns:
            count = int(result[missing_col].iloc[0])
            pct = float(round(count / total * 100, 2)) if total > 0 else 0.0
            missing_dict[col_name] = count
            missing_pct_dict[col_name] = pct
    
    return {
        "missing_values": [
            {
                "column_name": "Missing Values per Column",
                **missing_dict
            },
            {
                "column_name": "Percentage of Missing Values per Column",
                **missing_pct_dict
            }
        ]
    }


# ======================================================
# OUTLIERS (BQ-OPTIMIZED - IQR METHOD)
# ======================================================
def compute_outliers_bq(bq_client, project, dataset, table, num_cols):
    """
    Detect outliers using BigQuery APPROX_QUANTILES (MUCH FASTER)
    """
    if not num_cols:
        return {"outliers": []}
    
    # Build query for all numeric columns at once
    quantile_selects = []
    for col in num_cols:
        quantile_selects.append(f"""
        STRUCT(
            '{col}' as column_name,
            APPROX_QUANTILES({col}, 100)[OFFSET(25)] as q1,
            APPROX_QUANTILES({col}, 100)[OFFSET(75)] as q3,
            COUNT(*) as total_count
        ) as {col}_stats
        """)
    
    query = f"""
    SELECT
        {', '.join(quantile_selects)}
    FROM `{project}.{dataset}.{table}`
    """
    
    try:
        result = bq_client.query(query).to_dataframe()
        row = result.iloc[0]
        
        outliers = []
        for col in num_cols:
            stats_col = f"{col}_stats"
            if stats_col in result.columns:
                stats = result[stats_col].iloc[0]
                q1 = stats.get('q1')
                q3 = stats.get('q3')
                total = stats.get('total_count')
                
                if q1 is not None and q3 is not None:
                    iqr = q3 - q1
                    lower = q1 - 1.5 * iqr
                    upper = q3 + 1.5 * iqr
                    
                    # Count outliers
                    count_query = f"""
                    SELECT COUNT(*) as outlier_count
                    FROM `{project}.{dataset}.{table}`
                    WHERE {col} < {lower} OR {col} > {upper}
                    """
                    count_result = bq_client.query(count_query).to_dataframe()
                    outlier_count = int(count_result['outlier_count'].iloc[0])
                    outlier_pct = float(round(outlier_count / total * 100, 2)) if total > 0 else 0.0
                    
                    if outlier_count > 0:
                        outliers.append({
                            "column_name": col,
                            "outlier_count": outlier_count,
                            "outlier_percentage": outlier_pct,
                            "method": "IQR"
                        })
        
        return {"outliers": outliers}
    
    except Exception as e:
        print(f"Warning: Could not compute outliers: {e}")
        return {"outliers": []}


# ======================================================
# CARDINALITY CHECK (FAST)
# ======================================================
def compute_cardinality(df):
    """
    Check cardinality of columns (from sample - fast)
    """
    cardinality = df.nunique().to_dict()
    
    low_cardinality = [k for k, v in cardinality.items() if v <= 10]
    high_cardinality = [k for k, v in cardinality.items() if v > 0.5 * len(df)]
    id_like_cardinality = [k for k, v in cardinality.items() if v == len(df)]
    
    return {
        "Cardinality Check": [
            {
                "Low-cardinality": low_cardinality,
                "High-cardinality": high_cardinality,
                "ID-like cardinality": id_like_cardinality
            }
        ]
    }


# ======================================================
# UNIVARIATE - NUMERICAL (BQ-OPTIMIZED)
# ======================================================
def compute_univariate_numerical_bq(bq_client, project, dataset, table, num_cols):
    """
    Compute univariate statistics for numerical columns using BigQuery
    """
    if not num_cols:
        return []
    
    # Build dynamic query for all numeric columns
    stat_selects = []
    for col in num_cols:
        stat_selects.append(f"""
        STRUCT(
            '{col}' as column_name,
            MIN({col}) as min,
            MAX({col}) as max,
            AVG({col}) as mean,
            APPROX_QUANTILES({col}, 2)[OFFSET(1)] as median,
            STDDEV({col}) as std,
            VAR_POP({col}) as variance,
            APPROX_QUANTILES({col}, 4)[OFFSET(1)] as q1,
            APPROX_QUANTILES({col}, 4)[OFFSET(3)] as q3
        ) as {col}_stats
        """)
    
    query = f"""
    SELECT
        {', '.join(stat_selects)}
    FROM `{project}.{dataset}.{table}`
    WHERE {' AND '.join([f'{col} IS NOT NULL' for col in num_cols])}
    """
    
    try:
        result = bq_client.query(query).to_dataframe()
        row = result.iloc[0]
        
        univariate_stats = []
        for col in num_cols:
            stats_col = f"{col}_stats"
            if stats_col in result.columns:
                stats = result[stats_col].iloc[0]
                
                # Handle NaN/None values
                def safe_float(val):
                    if val is None or (isinstance(val, float) and np.isnan(val)):
                        return 0.0
                    return float(val)
                
                q1 = safe_float(stats.get('q1'))
                q3 = safe_float(stats.get('q3'))
                iqr = q3 - q1
                
                stats_dict = {
                    "column_name": col,
                    "min": safe_float(stats.get('min')),
                    "max": safe_float(stats.get('max')),
                    "mean": safe_float(stats.get('mean')),
                    "median": safe_float(stats.get('median')),
                    "std": safe_float(stats.get('std')),
                    "variance": safe_float(stats.get('variance')),
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "skewness": 0.0,  # BigQuery doesn't have native SKEWNESS
                    "kurtosis": 0.0   # BigQuery doesn't have native KURTOSIS
                }
                
                univariate_stats.append(stats_dict)
        
        return univariate_stats
    
    except Exception as e:
        print(f"Warning: Could not compute univariate numerical stats: {e}")
        return []


# ======================================================
# UNIVARIATE - CATEGORICAL (FAST - from sample)
# ======================================================
def compute_univariate_categorical(df, cat_cols):
    """
    Compute univariate statistics for categorical columns (from sample)
    """
    univariate_stats = []
    
    for col in cat_cols:
        series = df[col].dropna()
        
        if len(series) == 0:
            continue
        
        top_5_freqs = series.value_counts().head(5)
        top_5_list = [
            {"value": str(k), "count": int(v)} 
            for k, v in top_5_freqs.items()
        ]
        
        stats_dict = {
            "column_name": col,
            "unique_count": int(series.nunique()),
            "mode": str(series.mode()[0]) if len(series.mode()) > 0 else None,
            "top_5_frequencies": top_5_list
        }
        
        univariate_stats.append(stats_dict)
    
    return univariate_stats


# ======================================================
# BIVARIATE - CORRELATIONS (BQ-OPTIMIZED)
# ======================================================
def compute_correlations_bq(bq_client, project, dataset, table, target, num_cols):
    """
    Compute correlations using BigQuery CORR function
    """
    numeric_features = [c for c in num_cols if c != target]
    
    if len(numeric_features) == 0:
        return {}
    
    # Build correlation query for all numeric features
    corr_selects = []
    for col in numeric_features:
        corr_selects.append(f"CORR({col}, {target}) as {col}_corr")
    
    query = f"""
    SELECT
        {', '.join(corr_selects)}
    FROM `{project}.{dataset}.{table}`
    WHERE {target} IS NOT NULL AND {' AND '.join([f'{col} IS NOT NULL' for col in numeric_features])}
    """
    
    try:
        result = bq_client.query(query).to_dataframe()
        row = result.iloc[0]
        
        correlations = {}
        for col in numeric_features:
            corr_col = f"{col}_corr"
            if corr_col in result.columns:
                corr_val = row[corr_col]
                correlations[col] = float(corr_val) if not pd.isna(corr_val) else 0.0
        
        return {"correlations": correlations}
    
    except Exception as e:
        print(f"Warning: Could not compute correlations: {e}")
        return {}


# ======================================================
# BIVARIATE - CHI-SQUARE (from sample - fast)
# ======================================================
def compute_chi2_scores(df, target, cat_cols):
    """
    Compute chi-square scores for categorical features (from sample)
    """
    scores = {}
    
    for col in cat_cols:
        if col == target:
            continue
            
        try:
            confusion = pd.crosstab(df[col], df[target])
            chi2, _, _, _ = chi2_contingency(confusion)
            scores[col] = float(chi2) if not np.isnan(chi2) else 0.0
        except Exception as e:
            print(f"Warning: Could not compute chi2 for {col}: {e}")
            scores[col] = 0.0
    
    return {
        "chi2_scores": scores
    }