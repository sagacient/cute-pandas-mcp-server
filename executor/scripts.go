// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package executor provides script templates for pandas operations.
package executor

import (
	"fmt"
	"strings"
)

// WrapScript wraps user script with file path mappings and imports.
func WrapScript(userScript string, fileMapping map[string]string) string {
	var sb strings.Builder

	// Write standard imports
	sb.WriteString(`#!/usr/bin/env python3
import sys
import os
import json
import pandas as pd
import numpy as np

# Suppress warnings for cleaner output
import warnings
warnings.filterwarnings('ignore')

# File path mapping (original path -> container path)
FILE_MAPPING = {
`)

	// Add file mapping
	for original, container := range fileMapping {
		sb.WriteString(fmt.Sprintf("    %q: %q,\n", original, container))
	}
	sb.WriteString("}\n\n")

	// Add helper function to resolve paths
	sb.WriteString(`def resolve_path(path):
    """Resolve original file path to container path."""
    if path in FILE_MAPPING:
        return FILE_MAPPING[path]
    # Check if it's already a container path
    if path.startswith('/data/'):
        return path
    # Try to find by basename
    basename = os.path.basename(path)
    for orig, container in FILE_MAPPING.items():
        if os.path.basename(container) == basename:
            return container
    return path

# Output directory for saving results
OUTPUT_DIR = '/output'

def save_output(df, filename, format='csv'):
    """Save DataFrame to output directory."""
    path = os.path.join(OUTPUT_DIR, filename)
    if format == 'csv':
        df.to_csv(path, index=False)
    elif format == 'json':
        df.to_json(path, orient='records', indent=2)
    elif format == 'parquet':
        df.to_parquet(path, index=False)
    print(f"Saved output to: {path}")
    return path

# ===== USER SCRIPT BEGINS =====
`)

	sb.WriteString(userScript)
	sb.WriteString("\n# ===== USER SCRIPT ENDS =====\n")

	return sb.String()
}

// ReadDataFrameScript generates a script to read and describe a DataFrame.
func ReadDataFrameScript(containerPath string, previewRows int) string {
	return fmt.Sprintf(`#!/usr/bin/env python3
import sys
import os
import json
import pandas as pd
import numpy as np

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

file_path = %q
preview_rows = %d

# Determine file type and read accordingly
ext = os.path.splitext(file_path)[1].lower()

try:
    if ext == '.csv':
        df = pd.read_csv(file_path)
    elif ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    elif ext == '.json':
        df = pd.read_json(file_path)
    elif ext == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        # Try CSV as default
        df = pd.read_csv(file_path)
    
    # Collect info
    result = {
        "shape": {"rows": df.shape[0], "columns": df.shape[1]},
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
        "null_counts": df.isnull().sum().to_dict(),
        "preview": df.head(preview_rows).to_dict(orient='records')
    }
    
    print("=== DataFrame Info ===")
    print(f"Shape: {result['shape']['rows']} rows × {result['shape']['columns']} columns")
    print(f"Memory Usage: {result['memory_usage_mb']:.2f} MB")
    print()
    print("=== Columns ===")
    for col in result['columns']:
        dtype = result['dtypes'][col]
        nulls = result['null_counts'][col]
        print(f"  {col}: {dtype} ({nulls} nulls)")
    print()
    print("=== Preview ===")
    print(df.head(preview_rows).to_string())
    print()
    print("=== JSON Output ===")
    print(json.dumps(result, default=str))
    
except Exception as e:
    print(f"Error reading file: {e}", file=sys.stderr)
    sys.exit(1)
`, containerPath, previewRows)
}

// AnalyzeDataScript generates a script to analyze data.
func AnalyzeDataScript(containerPath string, analysisType string, columns []string, groupBy string) string {
	columnsJSON := "None"
	if len(columns) > 0 {
		columnsJSON = fmt.Sprintf("%q", strings.Join(columns, `", "`))
		columnsJSON = "[" + columnsJSON + "]"
	}

	groupByStr := "None"
	if groupBy != "" {
		groupByStr = fmt.Sprintf("%q", groupBy)
	}

	return fmt.Sprintf(`#!/usr/bin/env python3
import sys
import os
import json
import pandas as pd
import numpy as np

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

file_path = %q
analysis_type = %q
columns = %s
group_by = %s

# Read file
ext = os.path.splitext(file_path)[1].lower()
try:
    if ext == '.csv':
        df = pd.read_csv(file_path)
    elif ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    elif ext == '.json':
        df = pd.read_json(file_path)
    elif ext == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        df = pd.read_csv(file_path)
except Exception as e:
    print(f"Error reading file: {e}", file=sys.stderr)
    sys.exit(1)

# Filter columns if specified
if columns:
    available_cols = [c for c in columns if c in df.columns]
    if not available_cols:
        print(f"Error: None of the specified columns exist. Available: {list(df.columns)}", file=sys.stderr)
        sys.exit(1)
    df_subset = df[available_cols]
else:
    df_subset = df

try:
    if analysis_type == 'describe':
        print("=== Statistical Description ===")
        print(df_subset.describe(include='all').to_string())
        
    elif analysis_type == 'info':
        print("=== DataFrame Info ===")
        print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"\nColumn Types:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
        print(f"\nMemory Usage: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
        print(f"\nNull Values:")
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                print(f"  {col}: {count} ({count/len(df)*100:.1f}%%)")
                
    elif analysis_type == 'corr':
        numeric_df = df_subset.select_dtypes(include=[np.number])
        if numeric_df.empty:
            print("Error: No numeric columns found for correlation analysis", file=sys.stderr)
            sys.exit(1)
        print("=== Correlation Matrix ===")
        print(numeric_df.corr().to_string())
        
    elif analysis_type == 'value_counts':
        print("=== Value Counts ===")
        for col in df_subset.columns:
            print(f"\n--- {col} ---")
            vc = df_subset[col].value_counts()
            if len(vc) > 20:
                print(f"(Showing top 20 of {len(vc)} unique values)")
                print(vc.head(20).to_string())
            else:
                print(vc.to_string())
                
    elif analysis_type == 'groupby':
        if not group_by:
            print("Error: group_by parameter required for groupby analysis", file=sys.stderr)
            sys.exit(1)
        if group_by not in df.columns:
            print(f"Error: Column '{group_by}' not found. Available: {list(df.columns)}", file=sys.stderr)
            sys.exit(1)
        
        print(f"=== Group By: {group_by} ===")
        numeric_cols = df_subset.select_dtypes(include=[np.number]).columns.tolist()
        if not numeric_cols:
            print("No numeric columns to aggregate")
            print(df.groupby(group_by).size().to_string())
        else:
            grouped = df.groupby(group_by)[numeric_cols].agg(['mean', 'sum', 'count'])
            print(grouped.to_string())
    else:
        print(f"Error: Unknown analysis type '{analysis_type}'", file=sys.stderr)
        sys.exit(1)
        
except Exception as e:
    print(f"Error during analysis: {e}", file=sys.stderr)
    sys.exit(1)
`, containerPath, analysisType, columnsJSON, groupByStr)
}

// TransformDataScript generates a script to transform data.
func TransformDataScript(containerPath string, operations []map[string]interface{}, outputFormat string) string {
	opsJSON, _ := jsonMarshal(operations)

	return fmt.Sprintf(`#!/usr/bin/env python3
import sys
import os
import json
import pandas as pd
import numpy as np

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

file_path = %q
operations = %s
output_format = %q

# Read file
ext = os.path.splitext(file_path)[1].lower()
try:
    if ext == '.csv':
        df = pd.read_csv(file_path)
    elif ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    elif ext == '.json':
        df = pd.read_json(file_path)
    elif ext == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        df = pd.read_csv(file_path)
except Exception as e:
    print(f"Error reading file: {e}", file=sys.stderr)
    sys.exit(1)

original_shape = df.shape
print(f"Original shape: {original_shape[0]} rows × {original_shape[1]} columns")
print()

# Apply operations
for i, op in enumerate(operations):
    op_type = op.get('type')
    print(f"Operation {i+1}: {op_type}")
    
    try:
        if op_type == 'filter':
            column = op['column']
            operator = op['operator']
            value = op['value']
            
            if operator == '==':
                df = df[df[column] == value]
            elif operator == '!=':
                df = df[df[column] != value]
            elif operator == '>':
                df = df[df[column] > value]
            elif operator == '>=':
                df = df[df[column] >= value]
            elif operator == '<':
                df = df[df[column] < value]
            elif operator == '<=':
                df = df[df[column] <= value]
            elif operator == 'contains':
                df = df[df[column].astype(str).str.contains(str(value), na=False)]
            elif operator == 'isin':
                df = df[df[column].isin(value if isinstance(value, list) else [value])]
            else:
                print(f"  Warning: Unknown operator '{operator}'")
            print(f"  Filtered on {column} {operator} {value}: {len(df)} rows remaining")
            
        elif op_type == 'select':
            columns = op['columns']
            df = df[columns]
            print(f"  Selected columns: {columns}")
            
        elif op_type == 'drop':
            columns = op['columns']
            df = df.drop(columns=columns)
            print(f"  Dropped columns: {columns}")
            
        elif op_type == 'sort':
            column = op['column']
            ascending = op.get('ascending', True)
            df = df.sort_values(by=column, ascending=ascending)
            print(f"  Sorted by {column} ({'ascending' if ascending else 'descending'})")
            
        elif op_type == 'rename':
            mapping = op['mapping']
            df = df.rename(columns=mapping)
            print(f"  Renamed columns: {mapping}")
            
        elif op_type == 'dropna':
            subset = op.get('subset')
            if subset:
                df = df.dropna(subset=subset)
                print(f"  Dropped NA in columns: {subset}, {len(df)} rows remaining")
            else:
                df = df.dropna()
                print(f"  Dropped all rows with NA: {len(df)} rows remaining")
                
        elif op_type == 'fillna':
            column = op.get('column')
            fill_value = op.get('fill_value', 0)
            if column:
                df[column] = df[column].fillna(fill_value)
                print(f"  Filled NA in {column} with {fill_value}")
            else:
                df = df.fillna(fill_value)
                print(f"  Filled all NA with {fill_value}")
                
        elif op_type == 'astype':
            column = op['column']
            dtype = op['dtype']
            df[column] = df[column].astype(dtype)
            print(f"  Converted {column} to {dtype}")
            
        elif op_type == 'head':
            n = op.get('n', 5)
            df = df.head(n)
            print(f"  Took first {n} rows")
            
        elif op_type == 'tail':
            n = op.get('n', 5)
            df = df.tail(n)
            print(f"  Took last {n} rows")
            
        elif op_type == 'sample':
            n = op.get('n')
            frac = op.get('frac')
            if n:
                df = df.sample(n=min(n, len(df)))
                print(f"  Sampled {len(df)} rows")
            elif frac:
                df = df.sample(frac=frac)
                print(f"  Sampled {len(df)} rows ({frac*100}%%)")
                
        elif op_type == 'unique':
            columns = op.get('columns')
            if columns:
                df = df.drop_duplicates(subset=columns)
            else:
                df = df.drop_duplicates()
            print(f"  Removed duplicates: {len(df)} rows remaining")
            
        else:
            print(f"  Warning: Unknown operation type '{op_type}'")
            
    except Exception as e:
        print(f"  Error in operation: {e}", file=sys.stderr)
        sys.exit(1)

print()
print(f"Final shape: {df.shape[0]} rows × {df.shape[1]} columns")

# Save output
output_file = f'/output/transformed.{output_format}'
try:
    if output_format == 'csv':
        df.to_csv(output_file, index=False)
    elif output_format == 'json':
        df.to_json(output_file, orient='records', indent=2)
    elif output_format == 'parquet':
        df.to_parquet(output_file, index=False)
    else:
        df.to_csv(output_file, index=False)
    print(f"\nOutput saved to: {output_file}")
except Exception as e:
    print(f"Error saving output: {e}", file=sys.stderr)
    sys.exit(1)

# Print preview
print("\n=== Preview (first 10 rows) ===")
print(df.head(10).to_string())
`, containerPath, string(opsJSON), outputFormat)
}

// jsonMarshal is a helper to marshal JSON without HTML escaping.
func jsonMarshal(v interface{}) ([]byte, error) {
	// Simple JSON marshal for operations
	switch val := v.(type) {
	case []map[string]interface{}:
		result := "["
		for i, m := range val {
			if i > 0 {
				result += ", "
			}
			result += mapToJSON(m)
		}
		result += "]"
		return []byte(result), nil
	default:
		return []byte("[]"), nil
	}
}

func mapToJSON(m map[string]interface{}) string {
	result := "{"
	first := true
	for k, v := range m {
		if !first {
			result += ", "
		}
		first = false
		result += fmt.Sprintf("%q: ", k)
		switch val := v.(type) {
		case string:
			result += fmt.Sprintf("%q", val)
		case bool:
			if val {
				result += "True"
			} else {
				result += "False"
			}
		case int, int64, float64:
			result += fmt.Sprintf("%v", val)
		case []string:
			result += "["
			for i, s := range val {
				if i > 0 {
					result += ", "
				}
				result += fmt.Sprintf("%q", s)
			}
			result += "]"
		case []interface{}:
			result += "["
			for i, item := range val {
				if i > 0 {
					result += ", "
				}
				switch itemVal := item.(type) {
				case string:
					result += fmt.Sprintf("%q", itemVal)
				case bool:
					if itemVal {
						result += "True"
					} else {
						result += "False"
					}
				default:
					result += fmt.Sprintf("%v", itemVal)
				}
			}
			result += "]"
		case map[string]interface{}:
			result += mapToJSON(val)
		default:
			result += fmt.Sprintf("%v", val)
		}
	}
	result += "}"
	return result
}
