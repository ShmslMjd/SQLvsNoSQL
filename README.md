# MongoDB vs PostgreSQL Database Comparison

## 📋 Overview

Direct performance comparison between **MongoDB** (NoSQL) and **PostgreSQL** (SQL) across three objectives: Schema Flexibility, Performance Analysis, and Data Integrity.

### Key Features
- Side-by-side comparison with MongoDB Atlas and PostgreSQL on Render
- Secure credential management via `.env` files
- Individual and combined visualizations
- JSON export of all results

## 🎯 Objectives

1. **Schema Flexibility**: Dynamic schema vs structured schema with migrations
2. **Performance Analysis**: CRUD operations across 1K, 5K, 10K datasets  
3. **Data Integrity**: Validation rules and transaction handling

## 🏗️ Files

```
database_comparison.py          # Main comparison script
.env                           # Database credentials (create this)
objective_1_schema_flexibility.png
objective_2_performance_analysis.png  
objective_3_data_integrity.png
mongodb_vs_postgresql_comprehensive_comparison.png
database_comparison_results.json
```

## 🛠️ Setup

### 1. Install Dependencies
```bash
pip install pymongo psycopg2-binary python-dotenv matplotlib
```

### 2. Create `.env` file
```env
# MongoDB Atlas
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/

# PostgreSQL on Render  
POSTGRES_HOST=your-postgres-host.render.com
POSTGRES_DATABASE=your_database_name
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_PORT=5432
```

### 3. Run Comparison
```bash
python database_comparison.py
```

## 📊 Results

### Performance Summary (10K Dataset)
| Operation | MongoDB | PostgreSQL | Winner |
|-----------|---------|------------|--------|
| **CREATE** | ~2.5s | ~3.1s | 🍃 MongoDB |
| **READ** | ~0.024s | ~0.018s | 🐘 PostgreSQL |
| **UPDATE** | ~0.003s | ~0.002s | 🐘 PostgreSQL |
| **DELETE** | ~0.012s | ~0.009s | 🐘 PostgreSQL |

### Schema Flexibility
- **MongoDB**: ✅ Dynamic schema (no migration needed)
- **PostgreSQL**: ❌ Requires table migration for new fields

### Data Integrity
- **Both**: ✅ Successfully validate data and handle transactions
- **MongoDB**: JSON Schema validation
- **PostgreSQL**: Table constraints + foreign keys

## 🔧 Troubleshooting

### Connection Issues
- Check database credentials in `.env` file
- Verify network connectivity and IP whitelisting
- Ensure database services are running

### Missing Packages
```bash
pip install pymongo psycopg2-binary python-dotenv matplotlib
```

### SSL Issues (PostgreSQL)
- For local: Change `sslmode='require'` to `sslmode='disable'`
- For cloud: Ensure SSL is properly configured

## 📈 Generated Files

- **Individual graphs**: 3 separate visualization files for each objective
- **Combined dashboard**: Comprehensive comparison overview  
- **JSON results**: Complete data export for analysis
- **Console output**: Detailed performance metrics and timing

---

*Database Management System Assignment - MongoDB vs PostgreSQL Comparison*
