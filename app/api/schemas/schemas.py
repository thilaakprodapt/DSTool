EDA_SCHEMA = {
  "type": "object",
  "properties": {
    "Data Overview": {
      "type": "object",
      "properties": {
        "shape": {
          "type": "object",
          "properties": {
            "rows": { "type": "number" },
            "columns": { "type": "number" }
          },
          "required": ["rows", "columns"]
        },
        "feature_types": {
          "type": "object",
          "properties": {
            "numerical": { "type": "array", "items": { "type": "string" } },
            "categorical": { "type": "array", "items": { "type": "string" } },
            "boolean": { "type": "array", "items": { "type": "string" } },
            "datetime": { "type": "array", "items": { "type": "string" } },
            "text": { "type": "array", "items": { "type": "string" } },
            "high_cardinality": { "type": "array", "items": { "type": "string" } }
          },
          "required": [
            "numerical",
            "categorical",
            "boolean",
            "datetime",
            "text",
            "high_cardinality"
          ]
        },
        "sample_data": {
          "type": "array",
          "items": { "type": "object" }
        }
      },
      "required": ["shape", "feature_types", "sample_data"]
    },

    "Data quality": {
      "type": "object",
      "properties": {
        "missing_values": {
          "type": "array",
          "items": { "type": "object" }
        },
        "outliers": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "column_name": { "type": "string" },
              "outlier_count": { "type": "number" },
              "outlier_percentage": { "type": "number" },
              "method": { "type": "string" }
            },
            "required": [
              "column_name",
              "outlier_count",
              "outlier_percentage",
              "method"
            ]
          }
        },
        "Cardinality Check": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "Low-cardinality": {
                "type": "array",
                "items": { "type": "string" }
              },
              "High-cardinality": {
                "type": "array",
                "items": { "type": "string" }
              },
              "ID-like cardinality": {
                "type": "array",
                "items": { "type": "string" }
              }
            },
            "required": [
              "Low-cardinality",
              "High-cardinality",
              "ID-like cardinality"
            ]
          }
        }
      },
      "required": ["missing_values", "outliers", "Cardinality Check"]
    },

    "Univariate Analysis": {
      "type": "object",
      "properties": {
        "numerical": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "column_name": { "type": "string" },
              "min": { "type": "number" },
              "max": { "type": "number" },
              "mean": { "type": "number" },
              "median": { "type": "number" },
              "std": { "type": "number" },
              "variance": { "type": "number" },
              "q1": { "type": "number" },
              "q3": { "type": "number" },
              "iqr": { "type": "number" },
              "skewness": { "type": "number" },
              "kurtosis": { "type": "number" }
            },
            "required": [
              "column_name",
              "min", "max", "mean", "median", "std",
              "variance", "q1", "q3", "iqr", "skewness", "kurtosis"
            ]
          }
        },
        "img_url": { "type": "string" },
        "summary": { "type": "string" },

        "categorical": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "column_name": { "type": "string" },
              "unique_count": { "type": "number" },
              "mode": { "type": "string" },
              "top_5_frequencies": {
                "type": "array",
                "items": {
                  "type": "object",
                  "properties": {
                    "value": { "type": "string" },
                    "count": { "type": "number" }
                  },
                  "required": ["value", "count"]
                }
              }
            },
            "required": ["column_name", "unique_count", "mode", "top_5_frequencies"]
          }
        },
        "img_url_cat": { "type": "string" },
        "summary_cat": { "type": "string" }
      },
      "required": [
        "numerical",
        "img_url",
        "summary",
        "categorical",
        "img_url_cat",
        "summary_cat"
      ]
    },

    "Bivariate Analysis": {
      "type": "object",
      "properties": {
        "Target_column": { "type": "string" },

        "Numerical vs Target": {
          "type": "object",
          "properties": {
            "img_url": { "type": "string" },
            "summary": { "type": "string" }
          },
          "required": ["img_url", "summary"]
        },

        "Categorical vs Target": {
          "type": "object",
          "properties": {
            "img_url": { "type": "string" },
            "summary": { "type": "string" }
          },
          "required": ["img_url", "summary"]
        }
      },
      "required": [
        "Target_column",
        "Numerical vs Target",
        "Categorical vs Target"
      ]
    },

    "Summary": {
      "type": "object",
      "properties": {
        "summary": { "type": "string" }
      },
      "required": ["summary"]
    }
  },

  "required": [
    "Data Overview",
    "Data quality",
    "Univariate Analysis",
    "Bivariate Analysis",
    "Summary"
  ]
}


FE_SCHEMA = {
  "type": "object",
  "required": [
    "feature_engineering",
    "imbalance_analysis",
    "bias_detection",
    "balancing_recommendations"
  ],
  "properties": {
    "feature_engineering": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["feature_name", "recommendations"],
        "properties": {
          "feature_name": { "type": "string" },
          "eda_insights": {
            "type": "object",
            "properties": {
              "distribution": { "type": "array", "items": { "type": "string" } },
              "missing_values": { "type": "array", "items": { "type": "string" } },
              "correlations": { "type": "array", "items": { "type": "string" } },
              "outliers": { "type": "array", "items": { "type": "string" } }
            }
          },
          "selected_inputs": {
            "type": "array",
            "items": { "type": "string" }
          },
          "recommendations": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["technique", "reason"],
              "properties": {
                "technique": { "type": "string" },
                "reason": { "type": "string" }
              }
            }
          }
        }
      }
    },

    "imbalance_analysis": {
      "type": "object",
      "required": [
        "target_column",
        "target_type",
        "class_distribution",
        "is_imbalanced",
        "severity",
        "reasoning"
      ],
      "properties": {
        "target_column": { "type": "string" },
        "target_type": { "type": "string" },
        "class_distribution": {
          "type": "array",
          "items": { "type": "string" }
        },
        "imbalance_ratio": { "type": ["number", "null"] },
        "is_imbalanced": { "type": "boolean" },
        "severity": { "type": "string" },
        "reasoning": { "type": "string" }
      }
    },

    "bias_detection": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["sensitive_feature", "bias_type", "risk_level", "evidence"],
        "properties": {
          "sensitive_feature": { "type": "string" },
          "bias_type": { "type": "string" },
          "risk_level": { "type": "string" },
          "evidence": { "type": "string" }
        }
      }
    },

    "balancing_recommendations": {
      "type": "object",
      "required": ["recommended", "techniques", "user_action_required"],
      "properties": {
        "recommended": { "type": "boolean" },
        "techniques": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["method", "applicable_when", "pros", "cons"],
            "properties": {
              "method": { "type": "string" },
              "applicable_when": { "type": "string" },
              "pros": { "type": "string" },
              "cons": { "type": "string" }
            }
          }
        },
        "user_action_required": { "type": "string" }
      }
    }
  }
}


Col_report_schema = {
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "column_name": { "type": "string" },
      "eda_details": { "type": "string", "maxLength": 20 },
      "fe_details": { "type": "string" },
      "leakage_details": { "type": "string", "maxLength": 20 },
      "dag_details": { 
        "type": ["string", "null"] 
      },
      "data": {
        "type": "object",
        "properties": {
          "eda": { "type": "object" },
          "fe": { "type": "object" },
          "leakage": { "type": "object" },
          "dag": { "type": "object" }
        },
        "required": ["eda", "fe", "leakage", "dag"]
      }
    },
    "required": [
      "column_name",
      "eda_details",
      "fe_details",
      "leakage_details",
      "dag_details",
      "data"
    ]
  }
}


BALANCE_SCHEMA = {
    "type": "object",
    "properties": {
        "balanced_data": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    # Columns will be dynamic based on the original dataset
                    # Use "additionalProperties" to accept any column from the source table
                },
                "additionalProperties": True  # allows all columns from original data
            }
        },
        "target_column": {"type": "string"},
        "target_type": {"type": "string"},  # categorical or continuous
        "balancing_method": {"type": "string"},
        "balancing_reason": {"type": "string"},
        "rare_values_handled": {"type": "number"},
        "imbalance_ratio_before": {"type": "number"},
        "imbalance_ratio_after": {"type": "number"},
        "original_class_distribution": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "class": {},
                    "count": {"type": "number"}
                },
                "required": ["class", "count"]
            }
        },
        "balanced_class_distribution": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "class": {},
                    "count": {"type": "number"}
                },
                "required": ["class", "count"]
            }
        },
        "random_seed": {"type": "number"},
        "user_selected": {"type": "boolean"},
        "user_preference": {"type": "string"}
    },
    "required": [
        "balanced_data",
        "target_column",
        "target_type",
        "balancing_method",
        "balancing_reason",
        "rare_values_handled",
        "imbalance_ratio_before",
        "imbalance_ratio_after"
    ]
}
