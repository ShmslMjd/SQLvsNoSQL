"""
MongoDB vs PostgreSQL Database Comparison Experiment
=====================================================
Comprehensive comparison of MongoDB (NoSQL) vs PostgreSQL (SQL) databases

This experiment covers:
Objective 1: Schema Flexibility & Data Structure Support
Objective 2: Performance Analysis (CRUD Operations)  
Objective 3: Data Integrity & Consistency (E-commerce Transactions)

Direct side-by-side comparison with visualization of results
"""

# MongoDB imports
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson import ObjectId

# PostgreSQL imports
import psycopg2
from psycopg2 import Error
from psycopg2.extras import RealDictCursor

# Common imports
import time
import random
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import uuid
import numpy as np

# Visualization imports
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    HAS_MATPLOTLIB = True
    print("✅ matplotlib: Available for visualizations")
except ImportError:
    HAS_MATPLOTLIB = False
    print("⚠️  matplotlib: Not available - will create text-based results")

load_dotenv()

class DatabaseComparison:
    def __init__(self):
        """Initialize both MongoDB and PostgreSQL connections"""
        self.results = {
            'mongodb': {'metrics': {}, 'errors': []},
            'postgresql': {'metrics': {}, 'errors': []}
        }
        
        print("🚀 MongoDB vs PostgreSQL Database Comparison")
        print("=" * 60)
        
        # Initialize MongoDB
        try:
            self.mongo_client = MongoClient(os.getenv("MONGODB_URI"))
            self.mongo_db = self.mongo_client["comparison_test"]
            print("✅ MongoDB: Connected successfully")
        except Exception as e:
            print(f"❌ MongoDB: Connection failed - {e}")
            self.mongo_client = None
            
        # Initialize PostgreSQL
        try:
            self.postgres_conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST"),
                database=os.getenv("POSTGRES_DATABASE"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                sslmode='require'
            )
            self.postgres_conn.autocommit = False
            self.postgres_cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            print("✅ PostgreSQL: Connected successfully")
        except Exception as e:
            print(f"❌ PostgreSQL: Connection failed - {e}")
            self.postgres_conn = None

    def clear_data(self):
        """Clear previous experiment data from both databases"""
        print("\n🧹 Clearing previous data...")
        
        # Clear MongoDB
        if self.mongo_client:
            try:
                self.mongo_db.drop_collection("products")
                self.mongo_db.drop_collection("performance_test")
                self.mongo_db.drop_collection("customers")
                self.mongo_db.drop_collection("orders")
                self.mongo_db.drop_collection("payments")
                self.mongo_db.drop_collection("inventory")
                print("   ✅ MongoDB: Data cleared")
            except Exception as e:
                print(f"   ⚠️  MongoDB: Clear warning - {e}")
        
        # Clear PostgreSQL
        if self.postgres_conn:
            try:
                tables = ["payments", "order_items", "orders", "customers", "inventory",
                         "product_analytics", "product_variants", "product_reviews", 
                         "products_complex", "products_enhanced", "products", "performance_test"]
                for table in tables:
                    try:
                        self.postgres_cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    except:
                        pass
                self.postgres_conn.commit()
                print("   ✅ PostgreSQL: Data cleared")
            except Exception as e:
                print(f"   ⚠️  PostgreSQL: Clear warning - {e}")

    # =================================================================
    # OBJECTIVE 1: SCHEMA FLEXIBILITY & DATA STRUCTURE SUPPORT
    # =================================================================
    
    def run_objective_1_schema_flexibility(self):
        """Compare schema flexibility between MongoDB and PostgreSQL"""
        print("\n" + "=" * 60)
        print("📋 OBJECTIVE 1: SCHEMA FLEXIBILITY COMPARISON")
        print("=" * 60)
        
        results = {'mongodb': {}, 'postgresql': {}}
        
        # Test 1: Basic Schema Creation
        print("\n📦 Test 1: Basic Product Schema Creation")
        print("-" * 50)
        
        # MongoDB - Basic Schema
        if self.mongo_client:
            print("\n🍃 MongoDB Test:")
            products_coll = self.mongo_db["products"]
            basic_products = []
            for i in range(1, 101):  # 100 products for better comparison
                product = {
                    "_id": f"basic_{i:03d}",
                    "name": f"Product {i}",
                    "price": round(random.uniform(10, 500), 2),
                    "created_at": datetime.now()
                }
                basic_products.append(product)
            
            start_time = time.time()
            result = products_coll.insert_many(basic_products)
            mongo_basic_time = time.time() - start_time
            
            print(f"   ✅ Inserted {len(result.inserted_ids)} products in {mongo_basic_time:.4f}s")
            print(f"   ⚡ Rate: {len(result.inserted_ids)/mongo_basic_time:.0f} docs/sec")
            results['mongodb']['basic_insertion'] = {
                'time': mongo_basic_time,
                'count': len(result.inserted_ids),
                'rate': len(result.inserted_ids)/mongo_basic_time
            }
        
        # PostgreSQL - Basic Schema
        if self.postgres_conn:
            print("\n🐘 PostgreSQL Test:")
            # Create table
            create_table_sql = """
            CREATE TABLE products (
                id VARCHAR(20) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
            try:
                self.postgres_cursor.execute(create_table_sql)
                self.postgres_cursor.execute("CREATE INDEX idx_price ON products(price)")
                self.postgres_cursor.execute("CREATE INDEX idx_created_at ON products(created_at)")
                self.postgres_conn.commit()
                
                # Insert data
                basic_products = []
                for i in range(1, 101):
                    basic_products.append((
                        f"basic_{i:03d}",
                        f"Product {i}",
                        round(random.uniform(10, 500), 2),
                        datetime.now()
                    ))
                
                start_time = time.time()
                self.postgres_cursor.executemany(
                    "INSERT INTO products (id, name, price, created_at) VALUES (%s, %s, %s, %s)",
                    basic_products
                )
                self.postgres_conn.commit()
                postgres_basic_time = time.time() - start_time
                
                print(f"   ✅ Inserted {len(basic_products)} products in {postgres_basic_time:.4f}s")
                print(f"   ⚡ Rate: {len(basic_products)/postgres_basic_time:.0f} docs/sec")
                results['postgresql']['basic_insertion'] = {
                    'time': postgres_basic_time,
                    'count': len(basic_products),
                    'rate': len(basic_products)/postgres_basic_time
                }
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                results['postgresql']['basic_insertion'] = {'error': str(e)}
        
        # Test 2: Schema Evolution
        print("\n🔄 Test 2: Schema Evolution - Adding New Fields")
        print("-" * 50)
        
        # MongoDB - Schema Evolution (Dynamic)
        if self.mongo_client:
            print("\n🍃 MongoDB Test (Dynamic Schema):")
            enhanced_products = []
            categories = ["electronics", "books", "clothing"]
            
            for i in range(1, 101):
                category = random.choice(categories)
                product = {
                    "_id": f"enhanced_{i:03d}",
                    "name": f"Enhanced {category.title()} {i}",
                    "price": round(random.uniform(20, 800), 2),
                    "category": category,
                    "created_at": datetime.now()
                }
                
                # Category-specific fields (dynamic schema)
                if category == "electronics":
                    product.update({
                        "brand": random.choice(["Apple", "Samsung", "Sony"]),
                        "warranty_years": random.choice([1, 2, 3]),
                        "specs": {
                            "weight_kg": round(random.uniform(0.5, 5.0), 1),
                            "color": random.choice(["Black", "White", "Silver"])
                        }
                    })
                elif category == "books":
                    product.update({
                        "author": f"Author {random.randint(1, 100)}",
                        "pages": random.randint(100, 500),
                        "isbn": f"978-{random.randint(1000000000, 9999999999)}",
                        "genres": random.sample(["Fiction", "Mystery", "Sci-Fi", "Romance"], k=2)
                    })
                else:  # clothing
                    product.update({
                        "sizes": random.sample(["XS", "S", "M", "L", "XL"], k=3),
                        "material": random.choice(["Cotton", "Polyester", "Wool"]),
                        "colors": random.sample(["Red", "Blue", "Green", "Black"], k=2)
                    })
                
                enhanced_products.append(product)
            
            start_time = time.time()
            result = products_coll.insert_many(enhanced_products)
            mongo_evolution_time = time.time() - start_time
            
            print(f"   ✅ Added {len(result.inserted_ids)} enhanced products in {mongo_evolution_time:.4f}s")
            print(f"   💡 No schema migration needed - fields added dynamically")
            results['mongodb']['schema_evolution'] = {
                'time': mongo_evolution_time,
                'count': len(result.inserted_ids),
                'migration_required': False
            }
        
        # PostgreSQL - Schema Evolution (Requires new table)
        if self.postgres_conn:
            print("\n🐘 PostgreSQL Test (Requires Schema Planning):")
            # Create enhanced table with all possible fields
            create_enhanced_sql = """
            CREATE TABLE products_enhanced (
                id VARCHAR(20) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                category VARCHAR(50) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                brand VARCHAR(100),
                warranty_years INT,
                weight_kg DECIMAL(5,2),
                color VARCHAR(50),
                author VARCHAR(200),
                pages INT,
                isbn VARCHAR(20),
                genres JSON,
                sizes JSON,
                material VARCHAR(100),
                colors JSON
            )
            """
            
            try:
                self.postgres_cursor.execute(create_enhanced_sql)
                self.postgres_cursor.execute("CREATE INDEX idx_category ON products_enhanced(category)")
                self.postgres_conn.commit()
                
                enhanced_products = []
                categories = ["electronics", "books", "clothing"]
                
                for i in range(1, 101):
                    category = random.choice(categories)
                    base_data = [
                        f"enhanced_{i:03d}",
                        f"Enhanced {category.title()} {i}",
                        round(random.uniform(20, 800), 2),
                        category,
                        datetime.now()
                    ]
                    
                    if category == "electronics":
                        enhanced_products.append(base_data + [
                            random.choice(["Apple", "Samsung", "Sony"]),
                            random.choice([1, 2, 3]),
                            round(random.uniform(0.5, 5.0), 1),
                            random.choice(["Black", "White", "Silver"]),
                            None, None, None, None, None, None, None
                        ])
                    elif category == "books":
                        enhanced_products.append(base_data + [
                            None, None, None, None,
                            f"Author {random.randint(1, 100)}",
                            random.randint(100, 500),
                            f"978-{random.randint(1000000000, 9999999999)}",
                            json.dumps(random.sample(["Fiction", "Mystery", "Sci-Fi", "Romance"], k=2)),
                            None, None, None
                        ])
                    else:  # clothing
                        enhanced_products.append(base_data + [
                            None, None, None, None, None, None, None, None,
                            json.dumps(random.sample(["XS", "S", "M", "L", "XL"], k=3)),
                            random.choice(["Cotton", "Polyester", "Wool"]),
                            json.dumps(random.sample(["Red", "Blue", "Green", "Black"], k=2))
                        ])
                
                start_time = time.time()
                self.postgres_cursor.executemany("""
                    INSERT INTO products_enhanced 
                    (id, name, price, category, created_at, brand, warranty_years, weight_kg, color, 
                     author, pages, isbn, genres, sizes, material, colors) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, enhanced_products)
                self.postgres_conn.commit()
                postgres_evolution_time = time.time() - start_time
                
                print(f"   ✅ Added {len(enhanced_products)} enhanced products in {postgres_evolution_time:.4f}s")
                print(f"   💡 Required new table with predefined schema - NULL values for unused fields")
                results['postgresql']['schema_evolution'] = {
                    'time': postgres_evolution_time,
                    'count': len(enhanced_products),
                    'migration_required': True
                }
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                results['postgresql']['schema_evolution'] = {'error': str(e)}
        
        # Store results
        self.results['mongodb']['objective_1'] = results['mongodb']
        self.results['postgresql']['objective_1'] = results['postgresql']
        
        return results

    # =================================================================
    # OBJECTIVE 2: PERFORMANCE ANALYSIS
    # =================================================================
    
    def run_objective_2_performance(self):
        """Compare CRUD performance between MongoDB and PostgreSQL"""
        print("\n" + "=" * 60)
        print("📊 OBJECTIVE 2: PERFORMANCE ANALYSIS COMPARISON")
        print("=" * 60)
        
        dataset_sizes = [1000, 5000, 10000]
        results = {'mongodb': {}, 'postgresql': {}}
        
        for size in dataset_sizes:
            print(f"\n🔄 Testing with {size:,} documents:")
            print("-" * 40)
            
            size_results = {'mongodb': {}, 'postgresql': {}}
            
            # MongoDB Performance Test
            if self.mongo_client:
                print(f"\n🍃 MongoDB - {size:,} documents:")
                perf_coll = self.mongo_db["performance_test"]
                perf_coll.drop()  # Clear previous data
                
                # Generate test data
                test_data = []
                categories = ["electronics", "books", "clothing", "home", "sports"]
                for i in range(1, size + 1):
                    doc = {
                        "_id": f"perf_{size}_{i:06d}",
                        "name": f"Performance Test Product {i}",
                        "price": round(random.uniform(10, 1000), 2),
                        "category": random.choice(categories),
                        "description": f"Test product {i} for performance evaluation",
                        "created_at": datetime.now() - timedelta(days=random.randint(0, 365)),
                        "stock": random.randint(0, 1000),
                        "rating": round(random.uniform(1.0, 5.0), 1),
                        "tags": random.sample(["new", "sale", "featured", "popular", "limited"], k=random.randint(1, 3))
                    }
                    test_data.append(doc)
                
                # CREATE Test
                start_time = time.time()
                result = perf_coll.insert_many(test_data)
                create_time = time.time() - start_time
                create_rate = len(result.inserted_ids) / create_time
                
                print(f"   📝 CREATE: {len(result.inserted_ids):,} docs in {create_time:.3f}s ({create_rate:.0f} docs/sec)")
                
                # READ Tests
                read_tests = [
                    ("Simple filter", {"category": "electronics"}),
                    ("Range query", {"price": {"$gte": 100, "$lte": 500}}),
                    ("Text search", {"name": {"$regex": "Product 1"}}),
                    ("Complex query", {"category": "electronics", "rating": {"$gte": 4.0}}),
                    ("Array contains", {"tags": {"$in": ["featured"]}})
                ]
                
                read_times = []
                for test_name, query in read_tests:
                    start_time = time.time()
                    count = perf_coll.count_documents(query)
                    query_time = time.time() - start_time
                    read_times.append(query_time)
                    print(f"   📖 {test_name}: {count} results in {query_time:.4f}s")
                
                avg_read_time = sum(read_times) / len(read_times)
                
                # UPDATE Tests
                start_time = time.time()
                update_result = perf_coll.update_many(
                    {"category": "electronics"}, 
                    {"$inc": {"price": 10}}
                )
                single_update_time = time.time() - start_time
                
                start_time = time.time()
                bulk_result = perf_coll.update_many(
                    {"rating": {"$lt": 3.0}}, 
                    {"$set": {"status": "review_needed", "updated_at": datetime.now()}}
                )
                bulk_update_time = time.time() - start_time
                
                print(f"   ✏️  UPDATE: Price update ({update_result.modified_count:,} docs) in {single_update_time:.4f}s")
                print(f"   ✏️  UPDATE: Status update ({bulk_result.modified_count:,} docs) in {bulk_update_time:.4f}s")
                
                # DELETE Test
                docs_before = perf_coll.count_documents({})
                start_time = time.time()
                delete_result = perf_coll.delete_many({
                    "created_at": {"$lt": datetime.now() - timedelta(days=300)}
                })
                delete_time = time.time() - start_time
                docs_after = perf_coll.count_documents({})
                
                print(f"   🗑️  DELETE: Removed {delete_result.deleted_count:,} docs in {delete_time:.4f}s")
                print(f"   📈 Final count: {docs_after:,} documents")
                
                size_results['mongodb'] = {
                    'create_time': create_time,
                    'create_rate': create_rate,
                    'avg_read_time': avg_read_time,
                    'single_update_time': single_update_time,
                    'bulk_update_time': bulk_update_time,
                    'delete_time': delete_time,
                    'docs_before_delete': docs_before,
                    'docs_after_delete': docs_after
                }
            
            # PostgreSQL Performance Test
            if self.postgres_conn:
                print(f"\n🐘 PostgreSQL - {size:,} documents:")
                
                # Create performance table
                try:
                    self.postgres_cursor.execute("DROP TABLE IF EXISTS performance_test")
                    create_table_sql = """
                    CREATE TABLE performance_test (
                        id VARCHAR(30) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        price DECIMAL(10,2) NOT NULL,
                        category VARCHAR(50) NOT NULL,
                        description TEXT,
                        created_at TIMESTAMP NOT NULL,
                        stock INT DEFAULT 0,
                        rating DECIMAL(3,1) DEFAULT 0.0,
                        tags JSON,
                        status VARCHAR(20) DEFAULT 'active',
                        updated_at TIMESTAMP
                    )
                    """
                    self.postgres_cursor.execute(create_table_sql)
                    self.postgres_cursor.execute("CREATE INDEX idx_perf_category ON performance_test(category)")
                    self.postgres_cursor.execute("CREATE INDEX idx_perf_price ON performance_test(price)")
                    self.postgres_cursor.execute("CREATE INDEX idx_perf_rating ON performance_test(rating)")
                    self.postgres_cursor.execute("CREATE INDEX idx_perf_created_at ON performance_test(created_at)")
                    self.postgres_conn.commit()
                    
                    # Generate test data
                    test_data = []
                    categories = ["electronics", "books", "clothing", "home", "sports"]
                    for i in range(1, size + 1):
                        test_data.append((
                            f"perf_{size}_{i:06d}",
                            f"Performance Test Product {i}",
                            round(random.uniform(10, 1000), 2),
                            random.choice(categories),
                            f"Test product {i} for performance evaluation",
                            datetime.now() - timedelta(days=random.randint(0, 365)),
                            random.randint(0, 1000),
                            round(random.uniform(1.0, 5.0), 1),
                            json.dumps(random.sample(["new", "sale", "featured", "popular", "limited"], k=random.randint(1, 3)))
                        ))
                    
                    # CREATE Test
                    start_time = time.time()
                    self.postgres_cursor.executemany("""
                        INSERT INTO performance_test 
                        (id, name, price, category, description, created_at, stock, rating, tags) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, test_data)
                    self.postgres_conn.commit()
                    create_time = time.time() - start_time
                    create_rate = len(test_data) / create_time
                    
                    print(f"   📝 CREATE: {len(test_data):,} docs in {create_time:.3f}s ({create_rate:.0f} docs/sec)")
                    
                    # READ Tests
                    read_tests = [
                        ("Simple filter", "SELECT COUNT(*) FROM performance_test WHERE category = 'electronics'"),
                        ("Range query", "SELECT COUNT(*) FROM performance_test WHERE price BETWEEN 100 AND 500"),
                        ("Text search", "SELECT COUNT(*) FROM performance_test WHERE name LIKE '%Product 1%'"),
                        ("Complex query", "SELECT COUNT(*) FROM performance_test WHERE category = 'electronics' AND rating >= 4.0"),
                        ("JSON contains", "SELECT COUNT(*) FROM performance_test WHERE tags::jsonb @> '\"featured\"'")
                    ]
                    
                    read_times = []
                    for test_name, query in read_tests:
                        start_time = time.time()
                        self.postgres_cursor.execute(query)
                        result = self.postgres_cursor.fetchone()
                        query_time = time.time() - start_time
                        read_times.append(query_time)
                        count = result['count'] if result else 0
                        print(f"   📖 {test_name}: {count} results in {query_time:.4f}s")
                    
                    avg_read_time = sum(read_times) / len(read_times)
                    
                    # UPDATE Tests
                    start_time = time.time()
                    self.postgres_cursor.execute("""
                        UPDATE performance_test 
                        SET price = price + 10 
                        WHERE category = 'electronics'
                    """)
                    update_count = self.postgres_cursor.rowcount
                    self.postgres_conn.commit()
                    single_update_time = time.time() - start_time
                    
                    start_time = time.time()
                    self.postgres_cursor.execute("""
                        UPDATE performance_test 
                        SET status = 'review_needed', updated_at = NOW() 
                        WHERE rating < 3.0
                    """)
                    bulk_count = self.postgres_cursor.rowcount
                    self.postgres_conn.commit()
                    bulk_update_time = time.time() - start_time
                    
                    print(f"   ✏️  UPDATE: Price update ({update_count:,} docs) in {single_update_time:.4f}s")
                    print(f"   ✏️  UPDATE: Status update ({bulk_count:,} docs) in {bulk_update_time:.4f}s")
                    
                    # DELETE Test
                    self.postgres_cursor.execute("SELECT COUNT(*) FROM performance_test")
                    docs_before = self.postgres_cursor.fetchone()['count']
                    
                    start_time = time.time()
                    self.postgres_cursor.execute("""
                        DELETE FROM performance_test 
                        WHERE created_at < NOW() - INTERVAL '300 days'
                    """)
                    deleted_count = self.postgres_cursor.rowcount
                    self.postgres_conn.commit()
                    delete_time = time.time() - start_time
                    
                    self.postgres_cursor.execute("SELECT COUNT(*) FROM performance_test")
                    docs_after = self.postgres_cursor.fetchone()['count']
                    
                    print(f"   🗑️  DELETE: Removed {deleted_count:,} docs in {delete_time:.4f}s")
                    print(f"   📈 Final count: {docs_after:,} documents")
                    
                    size_results['postgresql'] = {
                        'create_time': create_time,
                        'create_rate': create_rate,
                        'avg_read_time': avg_read_time,
                        'single_update_time': single_update_time,
                        'bulk_update_time': bulk_update_time,
                        'delete_time': delete_time,
                        'docs_before_delete': docs_before,
                        'docs_after_delete': docs_after
                    }
                    
                except Exception as e:
                    print(f"   ❌ Error: {e}")
                    size_results['postgresql'] = {'error': str(e)}
            
            results['mongodb'][size] = size_results['mongodb']
            results['postgresql'][size] = size_results['postgresql']
        
        # Store results
        self.results['mongodb']['objective_2'] = results['mongodb']
        self.results['postgresql']['objective_2'] = results['postgresql']
        
        return results

    # =================================================================
    # OBJECTIVE 3: DATA INTEGRITY & CONSISTENCY
    # =================================================================
    
    def run_objective_3_data_integrity(self):
        """Compare data integrity and consistency features"""
        print("\n" + "=" * 60)
        print("🛡️  OBJECTIVE 3: DATA INTEGRITY & CONSISTENCY COMPARISON")
        print("=" * 60)
        
        results = {'mongodb': {}, 'postgresql': {}}
        
        # MongoDB Data Integrity Test
        if self.mongo_client:
            print("\n🍃 MongoDB Data Integrity Test:")
            print("-" * 40)
            
            # Setup validation schema
            self.mongo_db.drop_collection("customers")
            self.mongo_db.drop_collection("orders")
            self.mongo_db.drop_collection("payments")
            
            # Create collections with validation
            customer_validator = {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["customer_id", "email", "name"],
                    "properties": {
                        "customer_id": {"bsonType": "string", "pattern": "^CUST_[0-9]{6}$"},
                        "email": {"bsonType": "string", "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
                        "name": {"bsonType": "string", "minLength": 2, "maxLength": 100}
                    }
                }
            }
            
            self.mongo_db.create_collection("customers", validator=customer_validator)
            customers_coll = self.mongo_db["customers"]
            orders_coll = self.mongo_db["orders"]
            payments_coll = self.mongo_db["payments"]
            
            # Test valid data insertion
            valid_customer = {
                "customer_id": "CUST_000001",
                "email": "test@example.com",
                "name": "Test Customer",
                "created_at": datetime.now()
            }
            
            try:
                customers_coll.insert_one(valid_customer)
                print("   ✅ Valid customer data accepted")
                mongo_valid_insertions = 1
            except Exception as e:
                print(f"   ❌ Valid data rejected: {e}")
                mongo_valid_insertions = 0
            
            # Test invalid data (should be rejected)
            invalid_customers = [
                {"customer_id": "INVALID", "email": "test@example.com", "name": "Test"},
                {"customer_id": "CUST_000002", "email": "not-an-email", "name": "Test"},
                {"customer_id": "CUST_000003", "email": "test2@example.com", "name": "X"}
            ]
            
            mongo_blocked_insertions = 0
            for invalid_customer in invalid_customers:
                try:
                    customers_coll.insert_one(invalid_customer)
                    print(f"   ❌ Invalid data accepted: {invalid_customer}")
                except Exception:
                    mongo_blocked_insertions += 1
                    print(f"   ✅ Invalid data correctly rejected")
            
            # Test transaction (multi-document ACID)
            print("\n   🔄 Testing Multi-Document Transactions:")
            try:
                with self.mongo_client.start_session() as session:
                    with session.start_transaction():
                        # Create order
                        order = {
                            "_id": "ORD_000001",
                            "customer_id": "CUST_000001",
                            "total": 100.00,
                            "status": "pending",
                            "created_at": datetime.now()
                        }
                        orders_coll.insert_one(order, session=session)
                        
                        # Create payment
                        payment = {
                            "_id": "PAY_000001",
                            "order_id": "ORD_000001",
                            "amount": 100.00,
                            "status": "completed",
                            "created_at": datetime.now()
                        }
                        payments_coll.insert_one(payment, session=session)
                        
                        print("   ✅ Multi-document transaction completed successfully")
                        mongo_transactions_success = 1
                        
            except Exception as e:
                print(f"   ❌ Transaction failed: {e}")
                mongo_transactions_success = 0
            
            results['mongodb'] = {
                'validation_supported': True,
                'valid_insertions': mongo_valid_insertions,
                'blocked_invalid_insertions': mongo_blocked_insertions,
                'transactions_supported': True,
                'successful_transactions': mongo_transactions_success
            }
        
        # PostgreSQL Data Integrity Test  
        if self.postgres_conn:
            print("\n🐘 PostgreSQL Data Integrity Test:")
            print("-" * 40)
            
            try:
                # Drop existing tables
                tables = ["payments", "orders", "customers"]
                for table in tables:
                    self.postgres_cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                
                # Create tables with constraints
                customer_schema = """
                CREATE TABLE customers (
                    customer_id VARCHAR(20) PRIMARY KEY,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    name VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    CONSTRAINT chk_customer_id CHECK (customer_id ~ '^CUST_[0-9]{6}$'),
                    CONSTRAINT chk_email CHECK (email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'),
                    CONSTRAINT chk_name_length CHECK (LENGTH(name) >= 2 AND LENGTH(name) <= 100)
                )
                """
                
                orders_schema = """
                CREATE TABLE orders (
                    order_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    total DECIMAL(10,2) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
                """
                
                payments_schema = """
                CREATE TABLE payments (
                    payment_id VARCHAR(20) PRIMARY KEY,
                    order_id VARCHAR(20) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id)
                )
                """
                
                self.postgres_cursor.execute(customer_schema)
                self.postgres_cursor.execute(orders_schema)
                self.postgres_cursor.execute(payments_schema)
                self.postgres_conn.commit()
                
                # Test valid data insertion
                try:
                    self.postgres_cursor.execute("""
                        INSERT INTO customers (customer_id, email, name, created_at) 
                        VALUES ('CUST_000001', 'test@example.com', 'Test Customer', NOW())
                    """)
                    self.postgres_conn.commit()
                    print("   ✅ Valid customer data accepted")
                    postgres_valid_insertions = 1
                except Exception as e:
                    print(f"   ❌ Valid data rejected: {e}")
                    postgres_valid_insertions = 0
                
                # Test invalid data (should be rejected)
                invalid_inserts = [
                    "INSERT INTO customers VALUES ('INVALID', 'test@example.com', 'Test', NOW())",
                    "INSERT INTO customers VALUES ('CUST_000002', 'not-an-email', 'Test', NOW())",
                    "INSERT INTO customers VALUES ('CUST_000003', 'test2@example.com', 'X', NOW())"
                ]
                
                postgres_blocked_insertions = 0
                for invalid_sql in invalid_inserts:
                    try:
                        self.postgres_cursor.execute(invalid_sql)
                        self.postgres_conn.commit()
                        print("   ❌ Invalid data accepted")
                    except Exception:
                        postgres_blocked_insertions += 1
                        self.postgres_conn.rollback()
                        print("   ✅ Invalid data correctly rejected")
                
                # Test transaction (ACID)
                print("\n   🔄 Testing ACID Transactions:")
                try:
                    self.postgres_cursor.execute("BEGIN")
                    
                    # Create order
                    self.postgres_cursor.execute("""
                        INSERT INTO orders (order_id, customer_id, total, status, created_at) 
                        VALUES ('ORD_000001', 'CUST_000001', 100.00, 'pending', NOW())
                    """)
                    
                    # Create payment
                    self.postgres_cursor.execute("""
                        INSERT INTO payments (payment_id, order_id, amount, status, created_at) 
                        VALUES ('PAY_000001', 'ORD_000001', 100.00, 'completed', NOW())
                    """)
                    
                    self.postgres_cursor.execute("COMMIT")
                    print("   ✅ ACID transaction completed successfully")
                    postgres_transactions_success = 1
                    
                except Exception as e:
                    print(f"   ❌ Transaction failed: {e}")
                    self.postgres_conn.rollback()
                    postgres_transactions_success = 0
                
                results['postgresql'] = {
                    'validation_supported': True,
                    'valid_insertions': postgres_valid_insertions,
                    'blocked_invalid_insertions': postgres_blocked_insertions,
                    'transactions_supported': True,
                    'successful_transactions': postgres_transactions_success
                }
                
            except Exception as e:
                print(f"   ❌ Error setting up PostgreSQL integrity test: {e}")
                results['postgresql'] = {'error': str(e)}
        
        # Store results
        self.results['mongodb']['objective_3'] = results['mongodb']
        self.results['postgresql']['objective_3'] = results['postgresql']
        
        return results

    def create_individual_objective_graphs(self):
        """Create individual graphs for each objective"""
        if not HAS_MATPLOTLIB:
            return
        
        print("\n📊 Creating Individual Objective Visualizations...")
        
        # Objective 1: Schema Flexibility Graph
        self.create_objective_1_graph()
        
        # Objective 2: Performance Analysis Graph  
        self.create_objective_2_graph()
        
        # Objective 3: Data Integrity Graph
        self.create_objective_3_graph()

    def create_objective_1_graph(self):
        """Create Schema Flexibility comparison graph"""
        print("   📋 Creating Objective 1: Schema Flexibility Graph...")
        
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        fig.suptitle('Objective 1: Schema Flexibility & Data Structure Support', fontsize=16, fontweight='bold')
        
        if 'objective_1' in self.results['mongodb'] and 'objective_1' in self.results['postgresql']:
            mongo_obj1 = self.results['mongodb']['objective_1']
            postgres_obj1 = self.results['postgresql']['objective_1']
            
            # Basic insertion rates comparison
            categories = ['Basic\nInsertion Rate\n(docs/sec)', 'Schema Evolution\nFlexibility\n(% no migration needed)']
            mongo_values = [
                mongo_obj1.get('basic_insertion', {}).get('rate', 0),
                100 if not mongo_obj1.get('schema_evolution', {}).get('migration_required', True) else 0
            ]
            postgres_values = [
                postgres_obj1.get('basic_insertion', {}).get('rate', 0),
                0 if postgres_obj1.get('schema_evolution', {}).get('migration_required', True) else 100
            ]
            
            x = np.arange(len(categories))
            width = 0.35
            
            bars1 = ax.bar(x - width/2, mongo_values, width, label='MongoDB', color='#47A248', alpha=0.8)
            bars2 = ax.bar(x + width/2, postgres_values, width, label='PostgreSQL', color='#336791', alpha=0.8)
            
            ax.set_title('Performance & Flexibility Comparison', fontsize=14, fontweight='bold')
            ax.set_ylabel('Rate (docs/sec) / Flexibility Score (%)', fontsize=12)
            ax.set_xticks(x)
            ax.set_xticklabels(categories)
            ax.legend()
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    if height > 0:
                        if bar in bars1 and bar.get_x() < 0.5:  # First category - insertion rate
                            label = f'{height:.0f} docs/sec'
                        else:  # Second category - flexibility percentage
                            label = f'{height:.0f}%'
                        ax.text(bar.get_x() + bar.get_width()/2., height,
                                label, ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('objective_1_schema_flexibility.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("   ✅ Saved: objective_1_schema_flexibility.png")

    def create_objective_2_graph(self):
        """Create Performance Analysis comparison graph"""
        print("   📊 Creating Objective 2: Performance Analysis Graph...")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('Objective 2: Performance Analysis (CRUD Operations)', fontsize=16, fontweight='bold')
        
        if 'objective_2' in self.results['mongodb'] and 'objective_2' in self.results['postgresql']:
            # CRUD Performance (10K dataset)
            mongo_obj2 = self.results['mongodb']['objective_2'].get(10000, {})
            postgres_obj2 = self.results['postgresql']['objective_2'].get(10000, {})
            
            operations = ['CREATE\n(Insert)', 'READ\n(Query Avg)', 'UPDATE\n(Single)', 'UPDATE\n(Bulk)', 'DELETE']
            mongo_times = [
                mongo_obj2.get('create_time', 0),
                mongo_obj2.get('avg_read_time', 0),
                mongo_obj2.get('single_update_time', 0),
                mongo_obj2.get('bulk_update_time', 0),
                mongo_obj2.get('delete_time', 0)
            ]
            postgres_times = [
                postgres_obj2.get('create_time', 0),
                postgres_obj2.get('avg_read_time', 0),
                postgres_obj2.get('single_update_time', 0),
                postgres_obj2.get('bulk_update_time', 0),
                postgres_obj2.get('delete_time', 0)
            ]
            
            x = np.arange(len(operations))
            width = 0.35
            
            bars1 = ax1.bar(x - width/2, mongo_times, width, label='MongoDB', color='#47A248', alpha=0.8)
            bars2 = ax1.bar(x + width/2, postgres_times, width, label='PostgreSQL', color='#336791', alpha=0.8)
            
            ax1.set_title('CRUD Performance (10,000 Documents)', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Time (seconds)', fontsize=12)
            ax1.set_xticks(x)
            ax1.set_xticklabels(operations)
            ax1.legend()
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    if height > 0:
                        ax1.text(bar.get_x() + bar.get_width()/2., height,
                                f'{height:.2f}s', ha='center', va='bottom', fontweight='bold', fontsize=9)
            
            # Scaling Performance
            dataset_sizes = [1000, 5000, 10000]
            mongo_create_rates = []
            postgres_create_rates = []
            
            for size in dataset_sizes:
                mongo_data = self.results['mongodb']['objective_2'].get(size, {})
                postgres_data = self.results['postgresql']['objective_2'].get(size, {})
                mongo_create_rates.append(mongo_data.get('create_rate', 0))
                postgres_create_rates.append(postgres_data.get('create_rate', 0))
            
            ax2.plot(dataset_sizes, mongo_create_rates, 'o-', color='#47A248', linewidth=3, 
                    markersize=10, label='MongoDB', markerfacecolor='#47A248', markeredgecolor='#2E7D32')
            ax2.plot(dataset_sizes, postgres_create_rates, 's-', color='#336791', linewidth=3, 
                    markersize=10, label='PostgreSQL', markerfacecolor='#336791', markeredgecolor='#1565C0')
            
            ax2.set_title('Insert Performance Scaling', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Dataset Size (documents)', fontsize=12)
            ax2.set_ylabel('Insert Rate (docs/sec)', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            
            # Add annotations
            for i, (size, mongo_rate, postgres_rate) in enumerate(zip(dataset_sizes, mongo_create_rates, postgres_create_rates)):
                if mongo_rate > 0:
                    ax2.annotate(f'{mongo_rate:.0f}', (size, mongo_rate), textcoords="offset points", 
                               xytext=(0,15), ha='center', fontweight='bold', color='#2E7D32', fontsize=11)
                if postgres_rate > 0:
                    ax2.annotate(f'{postgres_rate:.0f}', (size, postgres_rate), textcoords="offset points", 
                               xytext=(0,-20), ha='center', fontweight='bold', color='#1565C0', fontsize=11)
        
        plt.tight_layout()
        plt.savefig('objective_2_performance_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("   ✅ Saved: objective_2_performance_analysis.png")

    def create_objective_3_graph(self):
        """Create Data Integrity comparison graph"""
        print("   🛡️  Creating Objective 3: Data Integrity Graph...")
        
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        fig.suptitle('Objective 3: Data Integrity & Consistency Test Results', fontsize=16, fontweight='bold')
        
        if 'objective_3' in self.results['mongodb'] and 'objective_3' in self.results['postgresql']:
            mongo_obj3 = self.results['mongodb']['objective_3']
            postgres_obj3 = self.results['postgresql']['objective_3']
            
            # Data validation results
            categories = ['Valid Data\nAccepted', 'Invalid Data\nBlocked', 'Transactions\nSuccessful']
            mongo_values = [
                mongo_obj3.get('valid_insertions', 0),
                mongo_obj3.get('blocked_invalid_insertions', 0),
                mongo_obj3.get('successful_transactions', 0)
            ]
            postgres_values = [
                postgres_obj3.get('valid_insertions', 0),
                postgres_obj3.get('blocked_invalid_insertions', 0),
                postgres_obj3.get('successful_transactions', 0)
            ]
            
            x = np.arange(len(categories))
            width = 0.35
            
            bars1 = ax.bar(x - width/2, mongo_values, width, label='MongoDB', color='#47A248', alpha=0.8)
            bars2 = ax.bar(x + width/2, postgres_values, width, label='PostgreSQL', color='#336791', alpha=0.8)
            
            ax.set_title('Data Integrity Test Results', fontsize=14, fontweight='bold')
            ax.set_ylabel('Count (Success Rate)', fontsize=12)
            ax.set_xticks(x)
            ax.set_xticklabels(categories)
            ax.legend()
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                            f'{int(height)}', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('objective_3_data_integrity.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("   ✅ Saved: objective_3_data_integrity.png")

    def create_comparison_visualizations(self):
        """Create side-by-side comparison visualizations"""
        if not HAS_MATPLOTLIB:
            self.create_text_comparison_report()
            return
        
        print("\n📊 Creating Comprehensive Comparison Visualization...")
        
        # Create comprehensive comparison chart
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
        fig.suptitle('MongoDB vs PostgreSQL - Complete Database Comparison', fontsize=20, fontweight='bold')
        
        # 1. Schema Flexibility Comparison
        if 'objective_1' in self.results['mongodb'] and 'objective_1' in self.results['postgresql']:
            mongo_obj1 = self.results['mongodb']['objective_1']
            postgres_obj1 = self.results['postgresql']['objective_1']
            
            # Basic insertion rates
            categories = ['Basic\nInsertion', 'Schema\nEvolution']
            mongo_rates = [
                mongo_obj1.get('basic_insertion', {}).get('rate', 0),
                1 / mongo_obj1.get('schema_evolution', {}).get('time', 1) * 100 if mongo_obj1.get('schema_evolution', {}).get('time') else 0
            ]
            postgres_rates = [
                postgres_obj1.get('basic_insertion', {}).get('rate', 0),
                1 / postgres_obj1.get('schema_evolution', {}).get('time', 1) * 100 if postgres_obj1.get('schema_evolution', {}).get('time') else 0
            ]
            
            x = np.arange(len(categories))
            width = 0.35
            
            bars1 = ax1.bar(x - width/2, mongo_rates, width, label='MongoDB', color='#47A248', alpha=0.8)
            bars2 = ax1.bar(x + width/2, postgres_rates, width, label='PostgreSQL', color='#336791', alpha=0.8)
            
            ax1.set_title('Schema Flexibility Performance', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Performance (docs/sec or relative)', fontsize=12)
            ax1.set_xticks(x)
            ax1.set_xticklabels(categories)
            ax1.legend()
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    if height > 0:
                        ax1.text(bar.get_x() + bar.get_width()/2., height,
                                f'{height:.0f}', ha='center', va='bottom', fontweight='bold')
        
        # 2. Performance Comparison (10K dataset)
        if 'objective_2' in self.results['mongodb'] and 'objective_2' in self.results['postgresql']:
            mongo_obj2 = self.results['mongodb']['objective_2'].get(10000, {})
            postgres_obj2 = self.results['postgresql']['objective_2'].get(10000, {})
            
            operations = ['CREATE', 'READ\n(avg)', 'UPDATE\n(single)', 'UPDATE\n(bulk)', 'DELETE']
            mongo_times = [
                mongo_obj2.get('create_time', 0),
                mongo_obj2.get('avg_read_time', 0),
                mongo_obj2.get('single_update_time', 0),
                mongo_obj2.get('bulk_update_time', 0),
                mongo_obj2.get('delete_time', 0)
            ]
            postgres_times = [
                postgres_obj2.get('create_time', 0),
                postgres_obj2.get('avg_read_time', 0),
                postgres_obj2.get('single_update_time', 0),
                postgres_obj2.get('bulk_update_time', 0),
                postgres_obj2.get('delete_time', 0)
            ]
            
            x = np.arange(len(operations))
            
            bars1 = ax2.bar(x - width/2, mongo_times, width, label='MongoDB', color='#47A248', alpha=0.8)
            bars2 = ax2.bar(x + width/2, postgres_times, width, label='PostgreSQL', color='#336791', alpha=0.8)
            
            ax2.set_title('CRUD Performance (10K Documents)', fontsize=14, fontweight='bold')
            ax2.set_ylabel('Time (seconds)', fontsize=12)
            ax2.set_xticks(x)
            ax2.set_xticklabels(operations)
            ax2.legend()
            ax2.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    if height > 0:
                        ax2.text(bar.get_x() + bar.get_width()/2., height,
                                f'{height:.3f}s', ha='center', va='bottom', fontweight='bold', fontsize=9)
        
        # 3. Scaling Performance
        if 'objective_2' in self.results['mongodb'] and 'objective_2' in self.results['postgresql']:
            dataset_sizes = [1000, 5000, 10000]
            mongo_create_rates = []
            postgres_create_rates = []
            
            for size in dataset_sizes:
                mongo_data = self.results['mongodb']['objective_2'].get(size, {})
                postgres_data = self.results['postgresql']['objective_2'].get(size, {})
                mongo_create_rates.append(mongo_data.get('create_rate', 0))
                postgres_create_rates.append(postgres_data.get('create_rate', 0))
            
            ax3.plot(dataset_sizes, mongo_create_rates, 'o-', color='#47A248', linewidth=3, 
                    markersize=8, label='MongoDB', markerfacecolor='#47A248', markeredgecolor='#2E7D32')
            ax3.plot(dataset_sizes, postgres_create_rates, 's-', color='#336791', linewidth=3, 
                    markersize=8, label='PostgreSQL', markerfacecolor='#336791', markeredgecolor='#1565C0')
            
            ax3.set_title('Insert Performance Scaling', fontsize=14, fontweight='bold')
            ax3.set_xlabel('Dataset Size (documents)', fontsize=12)
            ax3.set_ylabel('Insert Rate (docs/sec)', fontsize=12)
            ax3.legend()
            ax3.grid(True, alpha=0.3)
            
            # Add annotations
            for i, (size, mongo_rate, postgres_rate) in enumerate(zip(dataset_sizes, mongo_create_rates, postgres_create_rates)):
                if mongo_rate > 0:
                    ax3.annotate(f'{mongo_rate:.0f}', (size, mongo_rate), textcoords="offset points", 
                               xytext=(0,10), ha='center', fontweight='bold', color='#2E7D32')
                if postgres_rate > 0:
                    ax3.annotate(f'{postgres_rate:.0f}', (size, postgres_rate), textcoords="offset points", 
                               xytext=(0,-15), ha='center', fontweight='bold', color='#1565C0')
        
        # 4. Data Integrity Comparison
        if 'objective_3' in self.results['mongodb'] and 'objective_3' in self.results['postgresql']:
            mongo_obj3 = self.results['mongodb']['objective_3']
            postgres_obj3 = self.results['postgresql']['objective_3']
            
            categories = ['Valid Data\nAccepted', 'Invalid Data\nBlocked', 'Transactions\nSuccessful']
            mongo_values = [
                mongo_obj3.get('valid_insertions', 0),
                mongo_obj3.get('blocked_invalid_insertions', 0),
                mongo_obj3.get('successful_transactions', 0)
            ]
            postgres_values = [
                postgres_obj3.get('valid_insertions', 0),
                postgres_obj3.get('blocked_invalid_insertions', 0),
                postgres_obj3.get('successful_transactions', 0)
            ]
            
            x = np.arange(len(categories))
            
            bars1 = ax4.bar(x - width/2, mongo_values, width, label='MongoDB', color='#47A248', alpha=0.8)
            bars2 = ax4.bar(x + width/2, postgres_values, width, label='PostgreSQL', color='#336791', alpha=0.8)
            
            ax4.set_title('Data Integrity & Consistency', fontsize=14, fontweight='bold')
            ax4.set_ylabel('Count', fontsize=12)
            ax4.set_xticks(x)
            ax4.set_xticklabels(categories)
            ax4.legend()
            ax4.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    ax4.text(bar.get_x() + bar.get_width()/2., height,
                            f'{int(height)}', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('mongodb_vs_postgresql_comprehensive_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("✅ Comprehensive comparison visualization saved: 'mongodb_vs_postgresql_comprehensive_comparison.png'")

    def create_text_comparison_report(self):
        """Create a text-based comparison report when matplotlib is not available"""
        print("\n" + "=" * 60)
        print("📊 MONGODB VS POSTGRESQL COMPARISON REPORT")
        print("=" * 60)
        
        # Objective 1 Summary
        if 'objective_1' in self.results['mongodb'] and 'objective_1' in self.results['postgresql']:
            print("\n📋 OBJECTIVE 1: SCHEMA FLEXIBILITY")
            print("-" * 40)
            
            mongo_obj1 = self.results['mongodb']['objective_1']
            postgres_obj1 = self.results['postgresql']['objective_1']
            
            print("Basic Insertion Performance:")
            if 'basic_insertion' in mongo_obj1:
                print(f"   🍃 MongoDB:    {mongo_obj1['basic_insertion']['rate']:.0f} docs/sec")
            if 'basic_insertion' in postgres_obj1:
                print(f"   🐘 PostgreSQL: {postgres_obj1['basic_insertion']['rate']:.0f} docs/sec")
            
            print("\nSchema Evolution:")
            mongo_migration = mongo_obj1.get('schema_evolution', {}).get('migration_required', 'N/A')
            postgres_migration = postgres_obj1.get('schema_evolution', {}).get('migration_required', 'N/A')
            print(f"   🍃 MongoDB:    Migration Required: {mongo_migration}")
            print(f"   🐘 PostgreSQL: Migration Required: {postgres_migration}")
        
        # Objective 2 Summary
        if 'objective_2' in self.results['mongodb'] and 'objective_2' in self.results['postgresql']:
            print("\n📊 OBJECTIVE 2: PERFORMANCE ANALYSIS")
            print("-" * 40)
            
            for size in [1000, 5000, 10000]:
                if size in self.results['mongodb']['objective_2'] and size in self.results['postgresql']['objective_2']:
                    mongo_obj2 = self.results['mongodb']['objective_2'][size]
                    postgres_obj2 = self.results['postgresql']['objective_2'][size]
                    
                    print(f"\n{size:,} Documents Performance:")
                    print(f"   CREATE Rate:")
                    print(f"      🍃 MongoDB:    {mongo_obj2.get('create_rate', 0):.0f} docs/sec")
                    print(f"      🐘 PostgreSQL: {postgres_obj2.get('create_rate', 0):.0f} docs/sec")
                    
                    print(f"   Average READ Time:")
                    print(f"      🍃 MongoDB:    {mongo_obj2.get('avg_read_time', 0):.4f} seconds")
                    print(f"      🐘 PostgreSQL: {postgres_obj2.get('avg_read_time', 0):.4f} seconds")
        
        # Objective 3 Summary
        if 'objective_3' in self.results['mongodb'] and 'objective_3' in self.results['postgresql']:
            print("\n🛡️  OBJECTIVE 3: DATA INTEGRITY & CONSISTENCY")
            print("-" * 40)
            
            mongo_obj3 = self.results['mongodb']['objective_3']
            postgres_obj3 = self.results['postgresql']['objective_3']
            
            print("Data Validation:")
            print(f"   🍃 MongoDB:")
            print(f"      Valid insertions: {mongo_obj3.get('valid_insertions', 0)}")
            print(f"      Invalid blocked:  {mongo_obj3.get('blocked_invalid_insertions', 0)}")
            print(f"   🐘 PostgreSQL:")
            print(f"      Valid insertions: {postgres_obj3.get('valid_insertions', 0)}")
            print(f"      Invalid blocked:  {postgres_obj3.get('blocked_invalid_insertions', 0)}")
            
            print("\nTransaction Support:")
            print(f"   🍃 MongoDB:    {mongo_obj3.get('successful_transactions', 0)} successful")
            print(f"   🐘 PostgreSQL: {postgres_obj3.get('successful_transactions', 0)} successful")

    def save_results_to_file(self):
        """Save comparison results to JSON file"""
        results_with_metadata = {
            'experiment_info': {
                'title': 'MongoDB vs PostgreSQL Database Comparison',
                'date': datetime.now().isoformat(),
                'objectives': [
                    'Schema Flexibility & Data Structure Support',
                    'Performance Analysis (CRUD Operations)',
                    'Data Integrity & Consistency'
                ]
            },
            'results': self.results
        }
        
        with open('database_comparison_results.json', 'w') as f:
            json.dump(results_with_metadata, f, indent=2, default=str)
        
        print("✅ Results saved to: 'database_comparison_results.json'")

    def run_full_comparison(self):
        """Run complete comparison of MongoDB vs PostgreSQL"""
        print("🚀 Starting comprehensive database comparison...")
        
        # Clear previous data
        self.clear_data()
        
        # Run all objectives
        print("\n🔄 Running Objective 1: Schema Flexibility...")
        self.run_objective_1_schema_flexibility()
        
        print("\n🔄 Running Objective 2: Performance Analysis...")
        self.run_objective_2_performance()
        
        print("\n🔄 Running Objective 3: Data Integrity...")
        self.run_objective_3_data_integrity()
        
        # Create individual objective graphs
        self.create_individual_objective_graphs()
        
        # Create comprehensive comparison visualization
        self.create_comparison_visualizations()
        
        # Save results
        self.save_results_to_file()
        
        print("\n🎉 Database comparison completed successfully!")
        print("📊 Check the generated visualization and JSON results file.")

def main():
    """Main function to run the database comparison"""
    try:
        comparison = DatabaseComparison()
        comparison.run_full_comparison()
    except KeyboardInterrupt:
        print("\n⚠️  Experiment interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during comparison: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
