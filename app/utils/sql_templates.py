"""
SQL Templates for Feature Engineering Transformations

This module provides pre-validated SQL templates for common feature engineering
techniques. These templates are safer and more reliable than AI-generated SQL.

Usage:
    from app.utils.sql_templates import SQLTemplateEngine
    
    engine = SQLTemplateEngine(project_id, dataset_id, table_name)
    sql = engine.render("standardization", column="age")
"""

from typing import Dict, Any, List, Optional
from string import Template
import re


class SQLTemplateEngine:
    """
    Engine for rendering SQL templates for feature engineering transformations.
    
    Supports:
        - Numerical: standardization, normalization, log_transformation, binning
        - Categorical: one_hot_encoding, label_encoding, frequency_encoding
        - Missing values: imputation (mean, median, mode, constant)
        - Outliers: clipping, winsorization
    """
    
    def __init__(self, project_id: str, dataset_id: str, source_table: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.source_table = source_table
        self.full_table_id = f"`{project_id}.{dataset_id}.{source_table}`"
    
    # =========================================================================
    # NUMERICAL TRANSFORMATIONS
    # =========================================================================
    
    STANDARDIZATION = """
    -- Standardization (Z-score normalization): (x - mean) / std
    (${column} - AVG(${column}) OVER()) / NULLIF(STDDEV(${column}) OVER(), 0) AS ${column}_standardized
    """
    
    MIN_MAX_NORMALIZATION = """
    -- Min-Max Normalization: (x - min) / (max - min)
    (${column} - MIN(${column}) OVER()) / NULLIF(MAX(${column}) OVER() - MIN(${column}) OVER(), 0) AS ${column}_normalized
    """
    
    LOG_TRANSFORMATION = """
    -- Log Transformation: log(x + 1) to handle zeros
    LOG(${column} + 1) AS ${column}_log
    """
    
    SQRT_TRANSFORMATION = """
    -- Square Root Transformation
    SQRT(ABS(${column})) AS ${column}_sqrt
    """
    
    POWER_TRANSFORMATION = """
    -- Power Transformation (Box-Cox approximation for positive values)
    POWER(${column}, ${power}) AS ${column}_power
    """
    
    BINNING = """
    -- Binning/Bucketing into quantiles
    NTILE(${num_bins}) OVER (ORDER BY ${column}) AS ${column}_bin
    """
    
    CUSTOM_BINNING = """
    -- Custom range binning
    CASE
        WHEN ${column} < ${bin1} THEN 'low'
        WHEN ${column} < ${bin2} THEN 'medium'
        WHEN ${column} < ${bin3} THEN 'high'
        ELSE 'very_high'
    END AS ${column}_binned
    """
    
    ROBUST_SCALING = """
    -- Robust Scaling: (x - median) / IQR
    (${column} - PERCENTILE_CONT(${column}, 0.5) OVER()) / 
    NULLIF(PERCENTILE_CONT(${column}, 0.75) OVER() - PERCENTILE_CONT(${column}, 0.25) OVER(), 0) AS ${column}_robust_scaled
    """
    
    # =========================================================================
    # CATEGORICAL TRANSFORMATIONS  
    # =========================================================================
    
    LABEL_ENCODING = """
    -- Label Encoding: convert categories to integers
    DENSE_RANK() OVER (ORDER BY COALESCE(CAST(${column} AS STRING), '')) - 1 AS ${column}_encoded
    """
    
    FREQUENCY_ENCODING = """
    -- Frequency Encoding: replace category with its frequency (NULL-safe)
    COUNT(*) OVER (PARTITION BY CAST(${column} AS STRING)) AS ${column}_freq
    """
    
    TARGET_ENCODING = """
    -- Target Encoding: replace category with mean of target
    AVG(${target_column}) OVER (PARTITION BY ${column}) AS ${column}_target_encoded
    """
    
    # Note: True one-hot encoding requires knowing unique values beforehand.
    # This is a simplified binary encoding for binary columns.
    # Cast to STRING to handle both BOOL and STRING column types.
    BINARY_ENCODING = """
    -- Binary Encoding: convert to 0/1 (handles both BOOL and STRING)
    CASE 
        WHEN CAST(${column} AS STRING) IN ('Yes', 'yes', 'YES', 'true', 'True', 'TRUE', '1') THEN 1 
        ELSE 0 
    END AS ${column}_binary
    """
    
    # Hash encoding for high-cardinality categoricals
    HASH_ENCODING = """
    -- Hash Encoding: convert to integer hash
    MOD(ABS(FARM_FINGERPRINT(CAST(${column} AS STRING))), ${num_buckets}) AS ${column}_hash
    """
    
    # =========================================================================
    # STRING TRANSFORMATIONS
    # =========================================================================
    
    LOWERCASE = """
    -- Convert to lowercase (handles any type)
    LOWER(CAST(${column} AS STRING)) AS ${column}_lower
    """
    
    UPPERCASE = """
    -- Convert to uppercase (handles any type)
    UPPER(CAST(${column} AS STRING)) AS ${column}_upper
    """
    
    TRIM = """
    -- Trim whitespace (handles any type)
    TRIM(CAST(${column} AS STRING)) AS ${column}_trimmed
    """
    
    STRING_LENGTH = """
    -- Get string length (handles any type)
    LENGTH(CAST(${column} AS STRING)) AS ${column}_length
    """
    
    EXTRACT_NUMBERS = """
    -- Extract first number from string (handles any type)
    SAFE_CAST(REGEXP_EXTRACT(CAST(${column} AS STRING), r'[0-9]+') AS INT64) AS ${column}_number
    """
    
    # =========================================================================
    # MISSING VALUE HANDLING
    # =========================================================================
    
    IMPUTE_MEAN = """
    -- Impute missing values with mean
    COALESCE(${column}, AVG(${column}) OVER()) AS ${column}_imputed
    """
    
    IMPUTE_MEDIAN = """
    -- Impute missing values with median
    COALESCE(${column}, PERCENTILE_CONT(${column}, 0.5) OVER()) AS ${column}_imputed
    """
    
    # Note: True mode imputation in a single SELECT is complex in BigQuery.
    # Using FIRST_VALUE with a simple partition - fills NULLs with first non-null value.
    IMPUTE_MODE = """
    -- Impute missing values with first non-null value (mode approximation)
    COALESCE(${column}, FIRST_VALUE(${column} IGNORE NULLS) OVER (ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS ${column}_imputed
    """
    
    IMPUTE_CONSTANT = """
    -- Impute missing values with a constant
    COALESCE(${column}, ${fill_value}) AS ${column}_imputed
    """
    
    MISSING_INDICATOR = """
    -- Create missing value indicator column
    CASE WHEN ${column} IS NULL THEN 1 ELSE 0 END AS ${column}_is_missing
    """
    
    # =========================================================================
    # OUTLIER HANDLING
    # =========================================================================
    
    CLIP_OUTLIERS = """
    -- Clip outliers to specified bounds
    GREATEST(LEAST(${column}, ${upper_bound}), ${lower_bound}) AS ${column}_clipped
    """
    
    CLIP_IQR = """
    -- Clip outliers using IQR method (1.5 * IQR)
    GREATEST(
        LEAST(
            ${column},
            PERCENTILE_CONT(${column}, 0.75) OVER() + 1.5 * (PERCENTILE_CONT(${column}, 0.75) OVER() - PERCENTILE_CONT(${column}, 0.25) OVER())
        ),
        PERCENTILE_CONT(${column}, 0.25) OVER() - 1.5 * (PERCENTILE_CONT(${column}, 0.75) OVER() - PERCENTILE_CONT(${column}, 0.25) OVER())
    ) AS ${column}_clipped
    """
    
    WINSORIZE = """
    -- Winsorization: cap at percentiles
    GREATEST(
        LEAST(${column}, PERCENTILE_CONT(${column}, ${upper_percentile}) OVER()),
        PERCENTILE_CONT(${column}, ${lower_percentile}) OVER()
    ) AS ${column}_winsorized
    """
    
    # =========================================================================
    # FEATURE INTERACTION
    # =========================================================================
    
    POLYNOMIAL_FEATURES = """
    -- Polynomial features (squared)
    POWER(${column}, 2) AS ${column}_squared
    """
    
    INTERACTION = """
    -- Feature interaction (multiplication)
    ${column1} * ${column2} AS ${column1}_x_${column2}
    """
    
    RATIO = """
    -- Feature ratio
    ${column1} / NULLIF(${column2}, 0) AS ${column1}_div_${column2}
    """
    
    # =========================================================================
    # DATE/TIME FEATURES
    # =========================================================================
    
    EXTRACT_DATE_PARTS = """
    -- Extract date components
    EXTRACT(YEAR FROM ${column}) AS ${column}_year,
    EXTRACT(MONTH FROM ${column}) AS ${column}_month,
    EXTRACT(DAY FROM ${column}) AS ${column}_day,
    EXTRACT(DAYOFWEEK FROM ${column}) AS ${column}_dayofweek
    """
    
    DATE_DIFF = """
    -- Date difference from reference
    DATE_DIFF(CURRENT_DATE(), ${column}, DAY) AS ${column}_days_ago
    """
    
    # =========================================================================
    # TEMPLATE REGISTRY
    # =========================================================================
    
    TEMPLATES: Dict[str, str] = {
        # Numerical
        "standardization": STANDARDIZATION,
        "normalization": MIN_MAX_NORMALIZATION,
        "min_max_normalization": MIN_MAX_NORMALIZATION,
        "log_transformation": LOG_TRANSFORMATION,
        "log_transform": LOG_TRANSFORMATION,
        "sqrt_transformation": SQRT_TRANSFORMATION,
        "power_transformation": POWER_TRANSFORMATION,
        "binning": BINNING,
        "bucketing": BINNING,
        "custom_binning": CUSTOM_BINNING,
        "robust_scaling": ROBUST_SCALING,
        
        # Categorical
        "label_encoding": LABEL_ENCODING,
        "ordinal_encoding": LABEL_ENCODING,
        "frequency_encoding": FREQUENCY_ENCODING,
        "target_encoding": TARGET_ENCODING,
        "binary_encoding": BINARY_ENCODING,
        "hash_encoding": HASH_ENCODING,
        "hashing": HASH_ENCODING,
        
        # String transformations
        "lowercase": LOWERCASE,
        "lower": LOWERCASE,
        "uppercase": UPPERCASE,
        "upper": UPPERCASE,
        "trim": TRIM,
        "string_length": STRING_LENGTH,
        "extract_numbers": EXTRACT_NUMBERS,
        
        # Missing values
        "impute_mean": IMPUTE_MEAN,
        "mean_imputation": IMPUTE_MEAN,
        "fill_mean": IMPUTE_MEAN,
        "impute_median": IMPUTE_MEDIAN,
        "median_imputation": IMPUTE_MEDIAN,
        "fill_median": IMPUTE_MEDIAN,
        "impute_mode": IMPUTE_MODE,
        "mode_imputation": IMPUTE_MODE,
        "fill_mode": IMPUTE_MODE,
        "impute_constant": IMPUTE_CONSTANT,
        "fill_constant": IMPUTE_CONSTANT,
        "fillna": IMPUTE_CONSTANT,
        "missing_indicator": MISSING_INDICATOR,
        "is_missing": MISSING_INDICATOR,
        
        # Outliers
        "clip_outliers": CLIP_OUTLIERS,
        "clip": CLIP_OUTLIERS,
        "clipping": CLIP_OUTLIERS,
        "clip_iqr": CLIP_IQR,
        "iqr_clipping": CLIP_IQR,
        "winsorize": WINSORIZE,
        "winsorization": WINSORIZE,
        "capping": WINSORIZE,
        
        # Feature engineering
        "polynomial_features": POLYNOMIAL_FEATURES,
        "squared": POLYNOMIAL_FEATURES,
        "square": POLYNOMIAL_FEATURES,
        "interaction": INTERACTION,
        "multiply": INTERACTION,
        "ratio": RATIO,
        "divide": RATIO,
        
        # Date/Time
        "extract_date_parts": EXTRACT_DATE_PARTS,
        "date_features": EXTRACT_DATE_PARTS,
        "datetime_features": EXTRACT_DATE_PARTS,
        "date_diff": DATE_DIFF,
        "days_since": DATE_DIFF,
        
        # =================================================================
        # FE-TO-TEMPLATE NAME MAPPING
        # Maps natural language names from Feature Engineering service 
        # to actual template names
        # =================================================================
        
        # One-hot encoding (maps to hash_encoding for dynamic approach)
        "one_hot_encoding": HASH_ENCODING,
        "one-hot_encoding": HASH_ENCODING,
        "one-hot encoding": HASH_ENCODING,
        "onehot_encoding": HASH_ENCODING,
        "onehot": HASH_ENCODING,
        "dummy_encoding": HASH_ENCODING,
        "dummy_variables": HASH_ENCODING,
        "get_dummies": HASH_ENCODING,
        
        # Natural language aliases from FE recommendations
        "median imputation": IMPUTE_MEDIAN,
        "mean imputation": IMPUTE_MEAN,
        "mode imputation": IMPUTE_MODE,
        "z-score normalization": STANDARDIZATION,
        "z_score_normalization": STANDARDIZATION,
        "min-max scaling": MIN_MAX_NORMALIZATION,
        "min_max_scaling": MIN_MAX_NORMALIZATION,
        "log transform": LOG_TRANSFORMATION,
        "logarithmic transformation": LOG_TRANSFORMATION,
        "box-cox transformation": POWER_TRANSFORMATION,
        "yeo-johnson transformation": POWER_TRANSFORMATION,
        "quartile binning": BINNING,
        "quantile binning": BINNING,
        "equal width binning": CUSTOM_BINNING,
        "discretization": BINNING,
        "integer encoding": LABEL_ENCODING,
        "category encoding": LABEL_ENCODING,
        "count encoding": FREQUENCY_ENCODING,
        "frequency count": FREQUENCY_ENCODING,
        "outlier removal": CLIP_IQR,
        "outlier clipping": CLIP_IQR,
        "outlier treatment": CLIP_IQR,
        "iqr method": CLIP_IQR,
        "feature scaling": STANDARDIZATION,
        "data normalization": MIN_MAX_NORMALIZATION,
    }
    
    # Default parameters for templates
    DEFAULT_PARAMS: Dict[str, Dict[str, Any]] = {
        "binning": {"num_bins": 4},
        "custom_binning": {"bin1": 25, "bin2": 50, "bin3": 75},
        "power_transformation": {"power": 0.5},
        "impute_constant": {"fill_value": 0},
        "clip_outliers": {"lower_bound": 0, "upper_bound": 100},
        "winsorize": {"lower_percentile": 0.05, "upper_percentile": 0.95},
        "binary_encoding": {"positive_value": "Yes"},
        "hash_encoding": {"num_buckets": 100},
        "one_hot_encoding": {"num_buckets": 50},  # For one-hot via hash
    }
    
    def get_available_templates(self) -> List[str]:
        """Return list of available template names."""
        return list(self.TEMPLATES.keys())
    
    def render(self, template_name: str, **kwargs) -> str:
        """
        Render a SQL template with given parameters.
        
        Args:
            template_name: Name of the template (e.g., 'standardization')
            **kwargs: Template parameters (e.g., column='age')
            
        Returns:
            Rendered SQL string
            
        Raises:
            ValueError: If template not found or required params missing
        """
        # Normalize template name
        template_name = template_name.lower().replace(" ", "_").replace("-", "_")
        
        if template_name not in self.TEMPLATES:
            raise ValueError(
                f"Unknown template: '{template_name}'. "
                f"Available templates: {self.get_available_templates()}"
            )
        
        template_str = self.TEMPLATES[template_name]
        
        # Merge default params with provided params
        params = self.DEFAULT_PARAMS.get(template_name, {}).copy()
        params.update(kwargs)
        
        # Use Template for safe substitution
        template = Template(template_str)
        
        try:
            rendered = template.substitute(params)
        except KeyError as e:
            raise ValueError(f"Missing required parameter: {e}")
        
        return rendered.strip()
    
    def render_select_statement(
        self, 
        transformations: List[Dict[str, Any]],
        include_original: bool = True
    ) -> str:
        """
        Render a complete SELECT statement with multiple transformations.
        
        Args:
            transformations: List of dicts with 'column_name', 'fe_method', and optional params
            include_original: Whether to include SELECT * for original columns
            
        Returns:
            Complete SELECT SQL statement
        """
        select_parts = []
        
        if include_original:
            select_parts.append("*")
        
        for t in transformations:
            column = t.get("column_name")
            method = t.get("fe_method")
            params = {k: v for k, v in t.items() if k not in ("column_name", "fe_method")}
            params["column"] = column
            
            try:
                rendered = self.render(method, **params)
                select_parts.append(rendered)
            except ValueError as e:
                # Log warning but continue with other transformations
                print(f"Warning: Skipping transformation for {column}: {e}")
        
        select_clause = ",\n    ".join(select_parts)
        
        return f"""
SELECT
    {select_clause}
FROM {self.full_table_id}
"""
    
    def render_create_table_as(
        self,
        target_dataset: str,
        target_table: str,
        transformations: List[Dict[str, Any]],
        include_original: bool = True
    ) -> str:
        """
        Render a CREATE TABLE AS SELECT statement.
        
        Args:
            target_dataset: Target dataset for the new table
            target_table: Target table name
            transformations: List of transformations to apply
            include_original: Whether to include original columns
            
        Returns:
            Complete CREATE TABLE AS SELECT statement
        """
        target_full = f"`{self.project_id}.{target_dataset}.{target_table}`"
        select_sql = self.render_select_statement(transformations, include_original)
        
        return f"""
CREATE OR REPLACE TABLE {target_full} AS
{select_sql}
"""


def validate_column_name(column: str) -> bool:
    """Validate that column name is safe (no SQL injection)."""
    # Only allow alphanumeric and underscore
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column))


def sanitize_column_name(column: str) -> str:
    """Sanitize column name to be safe for SQL."""
    # Remove any non-alphanumeric characters except underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', column)
    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    return sanitized
