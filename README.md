# MongoDB Database Evaluation Experiment

## 📋 Overview

This project contains a comprehensive MongoDB evaluation experiment designed for comparing SQL vs NoSQL database systems. The experiment demonstrates MongoDB's capabilities across three key objectives:

1. **Schema Flexibility & Data Structure Support**
2. **Performance Analysis (CRUD Operations)**  
3. **Data Integrity & Consistency (E-commerce Transactions)**

## 🎯 Experiment Objectives

### Objective 1: Schema Flexibility & Data Structure Support
- Tests MongoDB's ability to handle evolving schemas without migration
- Demonstrates native support for nested objects, arrays, and mixed data types
- Evaluates query flexibility across different document structures
- **Key Tests**: Basic schema, schema evolution, complex nested structures, query flexibility

### Objective 2: Performance Analysis
- Measures CRUD (Create, Read, Update, Delete) operation performance
- Tests scaling characteristics across different dataset sizes (1K, 5K, 10K documents)
- Compares performance with equivalent SQL operations
- **Key Metrics**: Insert rates, query response times, update performance, deletion efficiency

### Objective 3: Data Integrity & Consistency
- Implements e-commerce order processing scenario with multi-document transactions
- Tests JSON Schema validation for business rules
- Demonstrates ACID compliance and rollback capabilities
- Evaluates referential integrity enforcement
- **Key Features**: Multi-document transactions, data validation, consistency vs performance trade-offs

## 🏗️ Project Structure

```
SQLvsNoSQL/
├── mongo_test.py                              # Main experiment script
├── mongodb_complete_evaluation_results.json   # Complete results export
├── objective_1_schema_flexibility.png        # Schema flexibility visualizations
├── objective_2_performance_analysis.png      # Performance analysis charts
├── objective_3_data_integrity.png           # Data integrity visualizations
├── .env                                      # MongoDB connection configuration
└── README.md                                # This file
```

## 🛠️ Prerequisites

### System Requirements
- **Python**: 3.8 or higher
- **MongoDB**: 4.4 or higher (with transaction support)
- **Operating System**: Windows, macOS, or Linux

### Required Python Packages
```bash
pip install pymongo python-dotenv matplotlib
```

### MongoDB Setup Options

#### Option 1: MongoDB Atlas (Cloud - Recommended)
1. Create a free account at [MongoDB Atlas](https://www.mongodb.com/atlas)
2. Create a new cluster
3. Get your connection string
4. Whitelist your IP address

#### Option 2: Local MongoDB Installation
1. Download and install MongoDB Community Server
2. Start MongoDB service
3. Use local connection string: `mongodb://localhost:27017`

## ⚙️ Installation & Setup

### 1. Clone or Download Project
```bash
# Download the project files to your desired directory
cd "path/to/your/project/folder"
```

### 2. Install Dependencies
```bash
# Install required Python packages
pip install pymongo python-dotenv matplotlib

# Alternative: Install specific versions
pip install pymongo==4.13.2 python-dotenv==1.0.0 matplotlib==3.10.5
```

### 3. Configure MongoDB Connection
Create a `.env` file in the project directory:

```env
# For MongoDB Atlas
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/

# For Local MongoDB
MONGODB_URI=mongodb://localhost:27017/
```

**Replace with your actual connection details:**
- `username`: Your MongoDB username
- `password`: Your MongoDB password
- `cluster.mongodb.net`: Your cluster URL

### 4. Verify Setup
Test your MongoDB connection:
```python
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv("MONGODB_URI"))
print("✅ MongoDB connected successfully!")
```

## 🚀 Running the Experiment

### Basic Execution
```bash
python mongo_test.py
```

### What Happens When You Run
1. **Initialization**: Connects to MongoDB and sets up collections
2. **Objective 1**: Tests schema flexibility (2-3 minutes)
3. **Objective 2**: Runs performance analysis (3-4 minutes)
4. **Objective 3**: Executes data integrity tests (2-3 minutes)
5. **Visualization**: Generates charts and exports results

### Expected Output
```
🚀 MongoDB Database Evaluation Experiment
============================================================
📋 Objectives: Schema Flexibility + Performance Analysis + Data Integrity
⏱️  Estimated time: 5-7 minutes

[Detailed test execution logs...]

✅ Individual objective visualizations created successfully!
📊 Generated files:
   • objective_1_schema_flexibility.png
   • objective_2_performance_analysis.png
   • objective_3_data_integrity.png
```

## 📊 Generated Output Files

### Visualization Files
- **`objective_1_schema_flexibility.png`**: Schema insertion performance, query flexibility, document distribution
- **`objective_2_performance_analysis.png`**: CRUD operations, scaling performance, metrics summary
- **`objective_3_data_integrity.png`**: Validation results, transaction success rates, consistency trade-offs

### Data Export
- **`mongodb_complete_evaluation_results.json`**: Complete experiment data in JSON format for SQL team comparison

## 📈 Key Experiment Results

### Schema Flexibility
- ✅ **No schema migration required** for new fields
- ✅ **Native support** for nested objects and arrays
- ✅ **Mixed document types** in same collection
- ✅ **Instant schema evolution** without downtime

### Performance Metrics
- 📊 **Insert Rate**: Up to 1,287 docs/second
- 🔍 **Query Time**: Average 0.024 seconds
- ✏️ **Update Performance**: Fast single and bulk operations
- 📈 **Scaling**: Good performance across 1K-10K datasets

### Data Integrity Features
- 🛡️ **JSON Schema Validation**: Business rule enforcement
- 🔄 **ACID Transactions**: Multi-document consistency
- 🔀 **Automatic Rollback**: Transaction failure handling
- 🔗 **Referential Integrity**: Application-level enforcement

## 🔧 Troubleshooting

### Common Issues

#### Connection Errors
```
pymongo.errors.ServerSelectionTimeoutError
```
**Solution**: Check your MongoDB URI and network connection

#### Authentication Failed
```
pymongo.errors.OperationFailure: Authentication failed
```
**Solution**: Verify username/password in `.env` file

#### Module Not Found
```
ModuleNotFoundError: No module named 'pymongo'
```
**Solution**: Install dependencies with `pip install pymongo python-dotenv matplotlib`

#### Visualization Issues
```
⚠️ matplotlib: Not available
```
**Solution**: Install matplotlib with `pip install matplotlib`

### Performance Considerations
- **Large Datasets**: Experiment uses moderate dataset sizes (1K-10K documents)
- **Memory Usage**: Approximately 50-100MB during execution
- **Network**: Atlas connections require stable internet

## 🔄 Customizing the Experiment

### Modifying Dataset Sizes
Edit `dataset_sizes` in the `test_crud_performance()` method:
```python
dataset_sizes = [500, 2000, 5000]  # Custom sizes
```

### Adding New Tests
Extend the `MongoDBExperiment` class with additional test methods:
```python
def test_custom_feature(self):
    # Your custom test implementation
    pass
```

### Changing Validation Rules
Modify JSON schemas in `setup_validation_schemas()` method for different business rules.

## 🤝 SQL Team Comparison

The experiment generates comprehensive data for SQL team comparison:

### Equivalent SQL Operations
- **INSERT**: Bulk insert operations
- **SELECT**: Range queries, filters, complex conditions
- **UPDATE**: Single and batch updates
- **DELETE**: Conditional deletions
- **Schema Changes**: ALTER TABLE vs instant field addition
- **Constraints**: CHECK constraints vs JSON Schema validation
- **Transactions**: BEGIN/COMMIT/ROLLBACK vs MongoDB transactions

### Comparison Points
1. **Schema Flexibility**: CREATE TABLE vs instant field addition
2. **Performance**: SQL INSERT/SELECT vs MongoDB operations
3. **Data Integrity**: FOREIGN KEY constraints vs application-level enforcement
4. **ACID Compliance**: SQL transactions vs MongoDB multi-document transactions

## 📚 Educational Value

This experiment demonstrates:
- **NoSQL vs SQL** trade-offs in real scenarios
- **Document database** advantages and considerations
- **Modern database features** like JSON validation and transactions
- **Performance evaluation** methodologies
- **Data integrity** approaches in NoSQL systems

## 🎓 Assignment Integration

Perfect for database management coursework covering:
- Database system comparison
- NoSQL database evaluation
- Performance analysis techniques
- Data integrity and consistency concepts
- Modern database technologies

## 📞 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Verify all prerequisites are installed
3. Ensure MongoDB connection is working
4. Review error messages for specific guidance

## 📄 License

This project is created for educational purposes as part of database management coursework.

---

**Created for Database Management System Assignment - SQL vs NoSQL Comparison**
