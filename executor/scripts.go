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
// If themeCode is non-empty, it is injected before the user script (e.g., matplotlib rcParams).
func WrapScript(userScript string, fileMapping map[string]string, themeCode string) string {
	var sb strings.Builder

	// Write standard imports
	sb.WriteString(`#!/usr/bin/env python3
import sys
import os
import json
import pandas as pd
import numpy as np
import duckdb
import polars as pl

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

def save_output(obj, filename, format=None):
    """
    Save various types of objects to output directory.
    
    Supports:
    - pandas DataFrame (csv, json, parquet, excel, xlsx)
    - matplotlib figure or plt module (png, jpg, svg, pdf)
    - BytesIO/file-like objects (pdf, images, any binary)
    - dict/list (json)
    - str (txt)
    - bytes (binary)
    
    Args:
        obj: Object to save. Can be:
             - DataFrame: pandas DataFrame
             - Figure: matplotlib figure (fig) or pyplot module (plt)
             - BytesIO/file-like: In-memory file objects (for reportlab, PIL, etc.)
             - dict/list: Python dict or list (saved as JSON)
             - str: Text string (saved as text file)
             - bytes: Binary data
        filename: Output filename (format auto-detected from extension)
        format: Optional format override (csv, json, png, etc.)
    
    Returns:
        str: Path to saved file
    
    Examples:
        save_output(df, 'data.csv')              # DataFrame to CSV
        save_output(fig, 'plot.png')             # Figure to PNG
        save_output(plt, 'current_plot.png')     # Current pyplot figure
        save_output({'key': 'val'}, 'data.json') # Dict to JSON
        
        # For PDFs with reportlab
        from io import BytesIO
        from reportlab.pdfgen import canvas
        buf = BytesIO()
        c = canvas.Canvas(buf)
        c.drawString(100, 750, 'Hello World')
        c.save()
        save_output(buf, 'report.pdf')          # BytesIO to PDF
    """
    import os
    path = os.path.join(OUTPUT_DIR, filename)
    
    # Auto-detect format from filename if not specified
    if format is None:
        format = os.path.splitext(filename)[1].lstrip('.').lower()
    
    # Handle pandas DataFrame
    if hasattr(obj, 'to_csv'):  # Duck typing for DataFrame
        if format in ['csv', 'txt']:
            obj.to_csv(path, index=False)
        elif format == 'json':
            obj.to_json(path, orient='records', indent=2)
        elif format in ['parquet', 'pq']:
            obj.to_parquet(path, index=False)
        elif format in ['xlsx', 'excel', 'xls']:
            obj.to_excel(path, index=False, engine='openpyxl')
        else:
            obj.to_csv(path, index=False)  # Default to CSV
    
    # Handle matplotlib figure or pyplot module
    elif hasattr(obj, 'savefig'):  # matplotlib figure
        obj.savefig(path, dpi=300, bbox_inches='tight')
    
    # Handle pyplot module - get current figure
    elif hasattr(obj, 'gcf'):  # matplotlib.pyplot module
        fig = obj.gcf()
        if fig.get_axes():  # Check if figure has content
            fig.savefig(path, dpi=300, bbox_inches='tight')
        else:
            raise ValueError("No active matplotlib figure to save. Use plt.figure() or pass fig object instead of plt module.")
    
    # Handle dict/list -> JSON
    elif isinstance(obj, (dict, list)):
        import json
        with open(path, 'w') as f:
            json.dump(obj, f, indent=2, default=str)
    
    # Handle string -> text file
    elif isinstance(obj, str):
        with open(path, 'w') as f:
            f.write(obj)
    
    # Handle bytes -> binary file
    elif isinstance(obj, bytes):
        with open(path, 'wb') as f:
            f.write(obj)
    
    # Handle BytesIO and file-like objects (for reportlab, PIL, etc.)
    elif hasattr(obj, 'read'):  # File-like object (BytesIO, StringIO, etc.)
        import io
        
        # Seek to beginning if possible
        if hasattr(obj, 'seek'):
            obj.seek(0)
        
        # Read content
        content = obj.read()
        
        # Determine binary vs text mode based on content type
        if isinstance(content, bytes):
            with open(path, 'wb') as f:
                f.write(content)
        else:
            with open(path, 'w') as f:
                f.write(content)
    
    else:
        raise TypeError(f"Unsupported type for save_output: {type(obj)}. Supported: DataFrame, Figure, plt module, dict, list, str, bytes, BytesIO/file-like objects")
    
    print(f"Saved output to: {path}")
    return path

def save_base64(base64_string, filename):
    """
    Save a base64-encoded string as a binary file.
    Useful when LLMs generate base64-encoded binary data.
    
    Args:
        base64_string: Base64-encoded string
        filename: Output filename
    
    Returns:
        str: Path to saved file
    
    Example:
        import base64
        pdf_bytes = generate_pdf()
        b64_string = base64.b64encode(pdf_bytes).decode()
        save_base64(b64_string, 'report.pdf')
    """
    import base64
    import os
    
    path = os.path.join(OUTPUT_DIR, filename)
    
    # Decode base64
    binary_data = base64.b64decode(base64_string)
    
    # Write to file
    with open(path, 'wb') as f:
        f.write(binary_data)
    
    print(f"Saved base64 output to: {path}")
    return path

# ===== USER SCRIPT BEGINS =====
`)

	// Inject chart theme code if provided
	if themeCode != "" {
		sb.WriteString("# ===== CHART THEME =====\n")
		sb.WriteString(themeCode)
		sb.WriteString("\n# ===== END CHART THEME =====\n\n")
	}

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

// WrapDuckDBScript generates a Python script that executes a SQL query using DuckDB.
// It auto-creates views for each mounted file and handles large result sets by
// saving full results to output files while returning summaries to stdout.
func WrapDuckDBScript(query string, fileMapping map[string]string, themeCode string) string {
	var sb strings.Builder

	sb.WriteString(`#!/usr/bin/env python3
import sys
import os
import json
import duckdb
import pandas as pd
import numpy as np

# Suppress warnings for cleaner output
import warnings
warnings.filterwarnings('ignore')

# File path mapping (original path -> container path)
FILE_MAPPING = {
`)

	for original, container := range fileMapping {
		sb.WriteString(fmt.Sprintf("    %q: %q,\n", original, container))
	}
	sb.WriteString("}\n\n")

	sb.WriteString(`def resolve_path(path):
    """Resolve original file path to container path."""
    if path in FILE_MAPPING:
        return FILE_MAPPING[path]
    if path.startswith('/data/'):
        return path
    basename = os.path.basename(path)
    for orig, container in FILE_MAPPING.items():
        if os.path.basename(container) == basename:
            return container
    return path

# Output directory for saving results
OUTPUT_DIR = '/output'

def save_output(obj, filename, format=None):
    """Save output to file. Supports DataFrame, dict, list, str, bytes, BytesIO."""
    path = os.path.join(OUTPUT_DIR, filename)
    if format is None:
        format = os.path.splitext(filename)[1].lstrip('.').lower()
    if hasattr(obj, 'to_csv'):
        if format in ['csv', 'txt']:
            obj.to_csv(path, index=False)
        elif format == 'json':
            obj.to_json(path, orient='records', indent=2)
        elif format in ['parquet', 'pq']:
            obj.to_parquet(path, index=False)
        elif format in ['xlsx', 'excel', 'xls']:
            obj.to_excel(path, index=False, engine='openpyxl')
        else:
            obj.to_csv(path, index=False)
    elif isinstance(obj, (dict, list)):
        with open(path, 'w') as f:
            json.dump(obj, f, indent=2, default=str)
    elif isinstance(obj, str):
        with open(path, 'w') as f:
            f.write(obj)
    elif isinstance(obj, bytes):
        with open(path, 'wb') as f:
            f.write(obj)
    elif hasattr(obj, 'read'):
        if hasattr(obj, 'seek'):
            obj.seek(0)
        content = obj.read()
        mode = 'wb' if isinstance(content, bytes) else 'w'
        with open(path, mode) as f:
            f.write(content)
    else:
        raise TypeError(f"Unsupported type for save_output: {type(obj)}")
    print(f"Saved output to: {path}")
    return path

# Initialize DuckDB connection
con = duckdb.connect()

# Auto-create views for each mounted file
_view_names = {}
for orig_path, container_path in FILE_MAPPING.items():
    basename = os.path.splitext(os.path.basename(container_path))[0]
    # Sanitize table name
    table_name = basename.replace('-', '_').replace(' ', '_').replace('.', '_')
    ext = os.path.splitext(container_path)[1].lower()
    try:
        if ext == '.csv':
            con.execute(f"CREATE VIEW \"{table_name}\" AS SELECT * FROM read_csv('{container_path}', auto_detect=true)")
        elif ext in ['.parquet', '.pq']:
            con.execute(f"CREATE VIEW \"{table_name}\" AS SELECT * FROM read_parquet('{container_path}')")
        elif ext == '.json':
            con.execute(f"CREATE VIEW \"{table_name}\" AS SELECT * FROM read_json('{container_path}', auto_detect=true)")
        elif ext in ['.xlsx', '.xls']:
            _df = pd.read_excel(container_path)
            con.register(table_name, _df)
        _view_names[orig_path] = table_name
    except Exception as e:
        print(f"Warning: Could not create view for {basename}: {e}", file=sys.stderr)

# Print available tables
if _view_names:
    print("Available tables:")
    for orig, tname in _view_names.items():
        print(f"  {tname} <- {os.path.basename(orig)}")
    print()

`)

	// Inject chart theme code if provided
	if themeCode != "" {
		sb.WriteString("# ===== CHART THEME =====\n")
		sb.WriteString(themeCode)
		sb.WriteString("\n# ===== END CHART THEME =====\n\n")
	}

	sb.WriteString("# ===== QUERY EXECUTION =====\n")
	sb.WriteString("try:\n")
	sb.WriteString(fmt.Sprintf("    _result = con.sql(%q)\n", query))
	sb.WriteString(`    _df = _result.df()

    print(f"Query returned {len(_df)} rows x {len(_df.columns)} columns")
    print()

    if len(_df) <= 200:
        # Small result: show everything
        print(_df.to_string())
    else:
        # Large result: save full data, show summary
        save_output(_df, 'query_result.csv')

        print(f"First 10 rows:")
        print(_df.head(10).to_string())
        print()
        print("Column statistics:")
        print(_df.describe(include='all').to_string())
        print()
        print(f"Full result saved to: /output/query_result.csv ({len(_df)} rows)")

except Exception as e:
    print(f"Query error: {e}", file=sys.stderr)
    sys.exit(1)
`)

	return sb.String()
}

// ProfileDataScript generates a Python script that produces a comprehensive
// profile of a dataset using DuckDB for speed on large files.
func ProfileDataScript(containerPath string) string {
	return fmt.Sprintf(`#!/usr/bin/env python3
import sys
import os
import json
import duckdb
import pandas as pd
import numpy as np

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

FILE_PATH = %q

# Detect file format and read with DuckDB
ext = os.path.splitext(FILE_PATH)[1].lower()
con = duckdb.connect()

try:
    if ext == '.csv':
        con.execute(f"CREATE VIEW data AS SELECT * FROM read_csv('{FILE_PATH}', auto_detect=true)")
    elif ext in ['.parquet', '.pq']:
        con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{FILE_PATH}')")
    elif ext == '.json':
        con.execute(f"CREATE VIEW data AS SELECT * FROM read_json('{FILE_PATH}', auto_detect=true)")
    elif ext in ['.xlsx', '.xls']:
        _df = pd.read_excel(FILE_PATH)
        con.register('data', _df)
    else:
        # Try CSV as fallback
        con.execute(f"CREATE VIEW data AS SELECT * FROM read_csv('{FILE_PATH}', auto_detect=true)")
except Exception as e:
    print(f"Error reading file: {e}", file=sys.stderr)
    sys.exit(1)

# Get basic info
row_count = con.execute("SELECT COUNT(*) FROM data").fetchone()[0]
col_info = con.execute("DESCRIBE data").fetchdf()
col_names = col_info['column_name'].tolist()
col_types = col_info['column_type'].tolist()
n_cols = len(col_names)

# File size
try:
    file_size = os.path.getsize(FILE_PATH)
    if file_size >= 1024 * 1024 * 1024:
        size_str = f"{file_size / (1024**3):.2f} GB"
    elif file_size >= 1024 * 1024:
        size_str = f"{file_size / (1024**2):.2f} MB"
    elif file_size >= 1024:
        size_str = f"{file_size / 1024:.1f} KB"
    else:
        size_str = f"{file_size} bytes"
except:
    size_str = "unknown"

print("=" * 60)
print("DATASET PROFILE")
print("=" * 60)
print(f"File: {os.path.basename(FILE_PATH)}")
print(f"Size: {size_str}")
print(f"Rows: {row_count:,}")
print(f"Columns: {n_cols}")
print()

# Per-column profiling
print("-" * 60)
print("COLUMN DETAILS")
print("-" * 60)

numeric_cols = []
categorical_cols = []

for col_name, col_type in zip(col_names, col_types):
    col_type_str = str(col_type).upper()
    print(f"\n  {col_name} ({col_type_str})")

    # Null count
    null_count = con.execute(f'SELECT COUNT(*) - COUNT("{col_name}") FROM data').fetchone()[0]
    null_pct = (null_count / row_count * 100) if row_count > 0 else 0
    print(f"    Nulls: {null_count:,} ({null_pct:.1f}%%)")

    # Cardinality
    distinct_count = con.execute(f'SELECT COUNT(DISTINCT "{col_name}") FROM data').fetchone()[0]
    print(f"    Distinct: {distinct_count:,}")

    is_numeric = any(t in col_type_str for t in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'BIGINT', 'SMALLINT', 'TINYINT', 'HUGEINT', 'REAL'])

    if is_numeric:
        numeric_cols.append(col_name)
        try:
            stats = con.execute(f'''
                SELECT
                    MIN("{col_name}") as min_val,
                    MAX("{col_name}") as max_val,
                    AVG("{col_name}")::DOUBLE as mean_val,
                    MEDIAN("{col_name}")::DOUBLE as median_val,
                    STDDEV("{col_name}")::DOUBLE as std_val,
                    QUANTILE_CONT("{col_name}", 0.25)::DOUBLE as q25,
                    QUANTILE_CONT("{col_name}", 0.75)::DOUBLE as q75
                FROM data
            ''').fetchone()
            print(f"    Min: {stats[0]}")
            print(f"    Max: {stats[1]}")
            print(f"    Mean: {stats[2]:.4f}" if stats[2] is not None else "    Mean: N/A")
            print(f"    Median: {stats[3]:.4f}" if stats[3] is not None else "    Median: N/A")
            print(f"    Std: {stats[4]:.4f}" if stats[4] is not None else "    Std: N/A")
            print(f"    Q25: {stats[5]:.4f}" if stats[5] is not None else "    Q25: N/A")
            print(f"    Q75: {stats[6]:.4f}" if stats[6] is not None else "    Q75: N/A")
        except Exception as e:
            print(f"    (stats error: {e})")
    else:
        categorical_cols.append(col_name)
        # Top 5 values
        try:
            top_vals = con.execute(f'''
                SELECT "{col_name}" as val, COUNT(*) as cnt
                FROM data
                WHERE "{col_name}" IS NOT NULL
                GROUP BY "{col_name}"
                ORDER BY cnt DESC
                LIMIT 5
            ''').fetchdf()
            if len(top_vals) > 0:
                print(f"    Top values:")
                for _, row in top_vals.iterrows():
                    pct = (row['cnt'] / row_count * 100)
                    print(f"      {row['val']}: {row['cnt']:,} ({pct:.1f}%%)")
        except Exception as e:
            print(f"    (top values error: {e})")

# Correlation matrix for numeric columns
if len(numeric_cols) >= 2:
    print()
    print("-" * 60)
    print("CORRELATIONS (numeric columns)")
    print("-" * 60)
    try:
        cols_str = ", ".join([f'"{c}"' for c in numeric_cols[:20]])  # Limit to 20 cols
        corr_df = con.execute(f"SELECT {cols_str} FROM data").fetchdf().corr()
        print(corr_df.to_string())
    except Exception as e:
        print(f"  (correlation error: {e})")

# Outlier detection (IQR method) for numeric columns
if numeric_cols:
    print()
    print("-" * 60)
    print("POTENTIAL OUTLIERS (IQR method)")
    print("-" * 60)
    for col_name in numeric_cols[:20]:  # Limit to 20 cols
        try:
            iqr_stats = con.execute(f'''
                SELECT
                    QUANTILE_CONT("{col_name}", 0.25)::DOUBLE as q1,
                    QUANTILE_CONT("{col_name}", 0.75)::DOUBLE as q3
                FROM data
            ''').fetchone()
            if iqr_stats[0] is not None and iqr_stats[1] is not None:
                q1, q3 = iqr_stats
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                outlier_count = con.execute(f'''
                    SELECT COUNT(*) FROM data
                    WHERE "{col_name}" < {lower} OR "{col_name}" > {upper}
                ''').fetchone()[0]
                if outlier_count > 0:
                    pct = (outlier_count / row_count * 100)
                    print(f"  {col_name}: {outlier_count:,} outliers ({pct:.1f}%%)")
        except:
            pass

# Sample rows
print()
print("-" * 60)
print("SAMPLE ROWS (first 5)")
print("-" * 60)
sample_df = con.execute("SELECT * FROM data LIMIT 5").fetchdf()
print(sample_df.to_string())
print()
`, containerPath)
}
