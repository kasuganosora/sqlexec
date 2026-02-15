# Excel Data Source

The Excel data source supports loading XLS and XLSX format spreadsheet files, allowing you to query Excel data using SQL.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `excel` |
| `database` | string | No | Database name this data source belongs to |

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `path` | _(required)_ | Excel file path (.xls or .xlsx) |
| `sheet` | _(first worksheet)_ | Worksheet name or index (starting from 0) |

## Table Name

The Excel data source table name is based on the worksheet name. For example, if the worksheet is named `Sheet1`, the table name is `Sheet1`.

```sql
SELECT * FROM Sheet1;
```

If the worksheet name contains spaces or special characters, use backticks:

```sql
SELECT * FROM `Sales Data 2025`;
```

## Data Parsing Rules

- **First row as column headers**: The first row of the worksheet will be parsed as column names.
- **Type inference**: SQLExec automatically infers column data types based on cell content.

| Excel Cell Type | Inferred Result |
|----------------|----------------|
| Numeric (integer) | `INT` |
| Numeric (decimal) | `FLOAT` |
| Boolean | `BOOLEAN` |
| Text | `TEXT` |
| Date | `TEXT` (formatted as string) |
| Empty | Inferred from other rows |

## Configuration Examples

### datasources.json

```json
{
  "datasources": [
    {
      "name": "sales",
      "type": "excel",
      "options": {
        "path": "/data/reports/sales_2025.xlsx",
        "sheet": "Q1Data"
      }
    },
    {
      "name": "inventory",
      "type": "excel",
      "options": {
        "path": "/data/inventory.xls"
      }
    }
  ]
}
```

### Query Examples

```sql
-- Switch to the Excel data source
USE sales;

-- Query a specific worksheet
SELECT * FROM Q1Data LIMIT 10;

-- Aggregate sales by product category
SELECT category, SUM(amount) AS total_sales
FROM Q1Data
GROUP BY category
ORDER BY total_sales DESC;

-- Conditional filtering
SELECT product_name, quantity, unit_price
FROM Q1Data
WHERE quantity > 100 AND unit_price < 50.00;
```

### Multi-Worksheet Queries

Configure multiple data sources pointing to different worksheets in the same file:

```json
{
  "datasources": [
    {
      "name": "sales_q1",
      "type": "excel",
      "options": {
        "path": "/data/reports/annual_2025.xlsx",
        "sheet": "Q1"
      }
    },
    {
      "name": "sales_q2",
      "type": "excel",
      "options": {
        "path": "/data/reports/annual_2025.xlsx",
        "sheet": "Q2"
      }
    }
  ]
}
```

## Notes

- The Excel data source is read-only by default and does not support write operations.
- The file is loaded into memory all at once during connection; be mindful of memory usage with large files.
- The first row must be column headers; worksheets without headers are not supported.
- Advanced Excel features such as merged cells and formulas may not be parsed correctly.
- Date-type cells will be formatted as text strings.
- Worksheets can be specified by name or by index number (starting from 0).
