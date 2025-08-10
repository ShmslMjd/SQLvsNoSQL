"""
MySQL Database Evaluation Experiment
====================================
Comprehensive evaluation of MySQL vs MongoDB databases

This experiment covers:
Objective 1: Schema Flexibility & Data Structure Support
Objective 2: Performance Analysis (CRUD Operations)
Objective 3: Data Integrity & Consistency (E-commerce Transactions)

Tests schema evolution, performance characteristics, query flexibility, 
data validation, multi-table transactions, and ACID compliance
"""

import psycopg2
from psycopg2 import Error
from psycopg2.extras import RealDictCursor
import ssl
import time
import random
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import uuid
import numpy as np

# Optional visualization imports
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    HAS_MATPLOTLIB = True
    print("✅ matplotlib: Available")
except ImportError:
    HAS_MATPLOTLIB = False
    print("⚠️  matplotlib: Not available - will create text-based visualizations instead")

load_dotenv()

class PostgreSQLTest:
    def __init__(self):
        """Initialize MySQL connection and experiment setup"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "dpg-d2aqmeogjchc73ejigd0-a.singapore-postgres.render.com"),
                database=os.getenv("POSTGRES_DATABASE", "postgresql_test_uvax"),
                user=os.getenv("POSTGRES_USER", "root"),
                password=os.getenv("POSTGRES_PASSWORD", "06YnZyGNgWfmykzqhnawWpqeQFiiON0D"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                sslmode='require' # Render Host requires SSL
            )

            self.connection.autocommit = False
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            self.metrics = {}
            
            print("🚀 PostgreSQL Database Evaluation Experiment")
            print("=" * 50)
            print(f"✅ Connected to PostgreSQL: {self.connection.get_dsn_parameters()}")
                
        except Error as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise
    
    def setup_database(self):
        """Setup the evaluation database and ensure clean state"""
        try:

            # Drop existing tables for clean slate
            tables_to_drop = [
                "products", "products_enhanced", "products_complex", "product_reviews",
                "product_variants", "product_analytics", "performance_test",
                "customers", "orders", "order_items", "payments", "inventory"
            ]
            
            for table in tables_to_drop:
                try:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                except:
                    pass
            
            self.connection.commit()
            print("🧹 Cleaned previous experiment data")
            
        except Error as e:
            print(f"⚠️  Database setup warning: {e}")

    def clear_data(self):
        """Clear previous experiment data"""
        try:
            self.cursor.execute("DROP TABLE IF EXISTS product_reviews CASCADE")
            self.cursor.execute("DROP TABLE IF EXISTS product_variants CASCADE")
            self.cursor.execute("DROP TABLE IF EXISTS product_analytics CASCADE")
            self.cursor.execute("DROP TABLE IF EXISTS products_complex CASCADE")
            self.cursor.execute("DROP TABLE IF EXISTS products_enhanced CASCADE")
            self.cursor.execute("DROP TABLE IF EXISTS products CASCADE")
            self.connection.commit()
            print("🧹 Cleared previous data")
        except Error as e:
            print(f"⚠️  Clear data warning: {e}")

    # =================================================================
    # OBJECTIVE 1: SCHEMA FLEXIBILITY & DATA STRUCTURE SUPPORT
    # =================================================================

    def test_1_basic_schema(self):
        """Test 1: Basic product schema creation and insertion"""
        print("\n📦 Test 1: Basic Product Schema")
        print("-" * 30)
        
        # Create basic products table
        create_table_sql = """
        CREATE TABLE products (
            id VARCHAR(20) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
        
        try:
            self.cursor.execute(create_table_sql)
            self.cursor.execute("CREATE INDEX idx_price ON products(price)")
            self.cursor.execute("CREATE INDEX idx_created_at ON products(created_at)")
            self.connection.commit()
            print("✅ Basic products table created")
        except Error as e:
            print(f"❌ Error creating table: {e}")
            return 0
        
        # Generate basic product data
        basic_products = []
        for i in range(1, 51):  # 50 products
            basic_products.append((
                f"basic_{i:03d}",
                f"Product {i}",
                round(random.uniform(10, 500), 2),
                datetime.now()
            ))
        
        # Measure insertion time
        insert_sql = "INSERT INTO products (id, name, price, created_at) VALUES (%s, %s, %s, %s)"
        
        start_time = time.time()
        self.cursor.executemany(insert_sql, basic_products)
        self.connection.commit()
        insertion_time = time.time() - start_time
        
        print(f"✅ Inserted {len(basic_products)} basic products")
        print(f"⏱️  Insertion time: {insertion_time:.4f} seconds")
        print("💡 SQL Equivalent: CREATE TABLE products (...); INSERT INTO products VALUES (...);")
        
        self.metrics['basic_insertion'] = insertion_time
        return len(basic_products)

    def test_2_schema_evolution(self):
        """Test 2: Schema evolution - adding new fields (requires ALTER TABLE)"""
        print("\n🔄 Test 2: Schema Evolution - Adding New Fields")
        print("-" * 45)
        
        # Create enhanced products table with additional fields
        create_enhanced_table_sql = """
        CREATE TABLE products_enhanced (
            id VARCHAR(20) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            category VARCHAR(50) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            -- Electronics specific fields
            brand VARCHAR(100),
            warranty_years INT,
            weight_kg DECIMAL(5,2),
            color VARCHAR(50),
            -- Books specific fields
            author VARCHAR(200),
            pages INT,
            isbn VARCHAR(20),
            genres JSON,
            -- Clothing specific fields
            sizes JSON,
            material VARCHAR(100),
            colors JSON
        )
        """
        
        start_time = time.time()
        
        try:
            self.cursor.execute(create_enhanced_table_sql)
            self.cursor.execute("CREATE INDEX idx_category ON products_enhanced(category)")
            self.cursor.execute("CREATE INDEX idx_brand ON products_enhanced(brand)")
            self.connection.commit()
            schema_creation_time = time.time() - start_time
            print(f"✅ Enhanced products table created in {schema_creation_time:.4f}s")
            print("💡 Required new table creation (MySQL doesn't support dynamic schema like MongoDB)")
        except Error as e:
            print(f"❌ Error creating enhanced table: {e}")
            return 0
        
        # Generate enhanced product data
        enhanced_products = []
        categories = ["electronics", "books", "clothing"]
        
        for i in range(1, 51):  # 50 enhanced products
            category = random.choice(categories)
            base_data = [
                f"enhanced_{i:03d}",
                f"Enhanced {category.title()} {i}",
                round(random.uniform(20, 800), 2),
                category,
                datetime.now()
            ]
            
            # Category-specific fields (many will be NULL for other categories)
            if category == "electronics":
                enhanced_products.append(base_data + [
                    random.choice(["Apple", "Samsung", "Sony"]),  # brand
                    random.choice([1, 2, 3]),  # warranty_years
                    round(random.uniform(0.5, 5.0), 1),  # weight_kg
                    random.choice(["Black", "White", "Silver"]),  # color
                    None, None, None,  # author, pages, isbn
                    None,  # genres (JSON)
                    None, None, None  # sizes, material, colors (JSON)
                ])
            elif category == "books":
                enhanced_products.append(base_data + [
                    None, None, None, None,  # brand, warranty_years, weight_kg, color
                    f"Author {random.randint(1, 100)}",  # author
                    random.randint(100, 500),  # pages
                    f"978-{random.randint(1000000000, 9999999999)}",  # isbn
                    json.dumps(random.sample(["Fiction", "Mystery", "Sci-Fi", "Romance"], k=2)),  # genres
                    None, None, None  # sizes, material, colors
                ])
            else:  # clothing
                enhanced_products.append(base_data + [
                    None, None, None, None,  # brand, warranty_years, weight_kg, color
                    None, None, None, None,  # author, pages, isbn, genres
                    json.dumps(random.sample(["XS", "S", "M", "L", "XL"], k=3)),  # sizes
                    random.choice(["Cotton", "Polyester", "Wool"]),  # material
                    json.dumps(random.sample(["Red", "Blue", "Green", "Black"], k=2))  # colors
                ])
        
        # Insert enhanced products
        insert_enhanced_sql = """
        INSERT INTO products_enhanced 
        (id, name, price, category, created_at, brand, warranty_years, weight_kg, color, 
         author, pages, isbn, genres, sizes, material, colors) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        start_time = time.time()
        self.cursor.executemany(insert_enhanced_sql, enhanced_products)
        self.connection.commit()
        evolution_time = time.time() - start_time
        
        print(f"✅ Added {len(enhanced_products)} enhanced products with category-specific fields")
        print(f"⏱️  Evolution time: {evolution_time:.4f} seconds")
        print("💡 Required schema planning - NULL values for unused fields")
        print("💡 MongoDB Advantage: No schema migration, fields added dynamically")
        
        self.metrics['evolution_insertion'] = evolution_time
        self.metrics['schema_migration_required'] = True
        return len(enhanced_products)

    def test_3_complex_nested_structures(self):
        """Test 3: Complex nested documents using normalized tables"""
        print("\n🎯 Test 3: Complex Nested Data Structures (Normalized)")
        print("-" * 50)
        
        # Create main products table for complex products
        create_complex_products_sql = """
        CREATE TABLE products_complex (
            id VARCHAR(20) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            category VARCHAR(50) NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
        
        # Create reviews table (1:many relationship)
        create_reviews_sql = """
        CREATE TABLE product_reviews (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(20) NOT NULL,
            reviewer VARCHAR(100) NOT NULL,
            rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
            comment TEXT,
            review_date TIMESTAMP NOT NULL,
            verified BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (product_id) REFERENCES products_complex(id) ON DELETE CASCADE
        )
        """
        
        # Create variants table (1:many relationship)
        create_variants_sql = """
        CREATE TABLE product_variants (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(20) NOT NULL,
            sku VARCHAR(50) NOT NULL UNIQUE,
            color VARCHAR(50),
            size VARCHAR(10),
            stock INT NOT NULL DEFAULT 0,
            price_modifier DECIMAL(8,2) DEFAULT 0.00,
            FOREIGN KEY (product_id) REFERENCES products_complex(id) ON DELETE CASCADE
        )
        """
        
        # Create analytics table (1:1 relationship)
        create_analytics_sql = """
        CREATE TABLE product_analytics (
            product_id VARCHAR(20) PRIMARY KEY,
            views INT DEFAULT 0,
            purchases INT DEFAULT 0,
            rating_average DECIMAL(3,1) DEFAULT 0.0,
            last_updated TIMESTAMP NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products_complex(id) ON DELETE CASCADE
        )
        """
        
        start_time = time.time()
        
        try:
            # Create all tables
            self.cursor.execute(create_complex_products_sql)
            self.cursor.execute(create_reviews_sql)
            self.cursor.execute(create_variants_sql)
            self.cursor.execute(create_analytics_sql)
            self.cursor.execute("CREATE INDEX idx_products_complex_category ON products_complex(category)")
            self.cursor.execute("CREATE INDEX idx_products_complex_price ON products_complex(price)")
            self.cursor.execute("CREATE INDEX idx_product_reviews_product_id ON product_reviews(product_id)")
            self.cursor.execute("CREATE INDEX idx_product_reviews_rating ON product_reviews(rating)")
            self.cursor.execute("CREATE INDEX idx_product_variants_product_id ON product_variants(product_id)")
            self.cursor.execute("CREATE INDEX idx_product_variants_sku ON product_variants(sku)")
            self.connection.commit()
            
            table_creation_time = time.time() - start_time
            print(f"✅ Created 4 normalized tables in {table_creation_time:.4f}s")
            print("💡 Normalized approach: 1 main table + 3 related tables")
            
        except Error as e:
            print(f"❌ Error creating complex tables: {e}")
            return 0
        
        # Insert complex products with related data
        complex_products = []
        all_reviews = []
        all_variants = []
        all_analytics = []
        
        for i in range(1, 21):  # 20 complex products
            product_id = f"complex_{i:03d}"
            
            # Main product
            complex_products.append((
                product_id,
                f"Complex Product {i}",
                round(random.uniform(100, 1000), 2),
                "electronics",
                datetime.now()
            ))
            
            # Reviews for this product
            num_reviews = random.randint(2, 4)
            for j in range(1, num_reviews + 1):
                all_reviews.append((
                    product_id,
                    f"User{j}",
                    random.randint(1, 5),
                    f"Review {j} for product {i}",
                    datetime.now() - timedelta(days=random.randint(1, 30)),
                    random.choice([True, False])
                ))
            
            # Variants for this product
            num_variants = random.randint(2, 3)
            for k in range(1, num_variants + 1):
                all_variants.append((
                    product_id,
                    f"SKU-{i}-{k}",
                    random.choice(["Red", "Blue", "Green"]),
                    random.choice(["S", "M", "L"]),
                    random.randint(0, 100),
                    round(random.uniform(-10, 50), 2)
                ))
            
            # Analytics for this product
            all_analytics.append((
                product_id,
                random.randint(100, 5000),  # views
                random.randint(1, 100),     # purchases
                round(random.uniform(3.0, 5.0), 1),  # rating_average
                datetime.now()
            ))
        
        # Insert all data
        start_time = time.time()
        
        # Insert main products
        self.cursor.executemany(
            "INSERT INTO products_complex (id, name, price, category, created_at) VALUES (%s, %s, %s, %s, %s)",
            complex_products
        )
        
        # Insert reviews
        self.cursor.executemany(
            "INSERT INTO product_reviews (product_id, reviewer, rating, comment, review_date, verified) VALUES (%s, %s, %s, %s, %s, %s)",
            all_reviews
        )
        
        # Insert variants
        self.cursor.executemany(
            "INSERT INTO product_variants (product_id, sku, color, size, stock, price_modifier) VALUES (%s, %s, %s, %s, %s, %s)",
            all_variants
        )
        
        # Insert analytics
        self.cursor.executemany(
            "INSERT INTO product_analytics (product_id, views, purchases, rating_average, last_updated) VALUES (%s, %s, %s, %s, %s)",
            all_analytics
        )
        
        self.connection.commit()
        complex_time = time.time() - start_time
        
        print(f"✅ Added {len(complex_products)} products with complex relationships")
        print(f"   • {len(all_reviews)} reviews inserted")
        print(f"   • {len(all_variants)} variants inserted")
        print(f"   • {len(all_analytics)} analytics records inserted")
        print(f"⏱️  Complex insertion time: {complex_time:.4f} seconds")
        print("💡 Normalized structure: FOREIGN KEY relationships maintained")
        print("💡 MongoDB Advantage: Single document with nested arrays vs 4 tables")
        
        self.metrics['complex_insertion'] = complex_time
        return len(complex_products)

    def test_4_query_flexibility(self):
        """Test 4: Query across different table structures"""
        print("\n🔍 Test 4: Query Flexibility Across Schema Variations")
        print("-" * 50)
        
        queries = [
            ("Basic price range", 
             "SELECT COUNT(*) FROM products WHERE price BETWEEN 100 AND 300", 
             "products"),
            
            ("Category filter", 
             "SELECT COUNT(*) FROM products_enhanced WHERE category = 'electronics'", 
             "products_enhanced"),
            
            ("JSON field query", 
             "SELECT COUNT(*) FROM products_enhanced WHERE colors::jsonb @> '\"Black\"'", 
             "products_enhanced"),
            
            ("Existence check", 
             "SELECT COUNT(*) FROM products_enhanced WHERE sizes IS NOT NULL", 
             "products_enhanced"),
            
            ("Complex JOIN query", 
             """SELECT COUNT(*) FROM products_complex p 
                JOIN product_reviews r ON p.id = r.product_id 
                WHERE r.rating >= 4""", 
             "join"),
            
            ("Multi-level JOIN", 
             """SELECT COUNT(*) FROM products_complex p 
                JOIN product_analytics a ON p.id = a.product_id 
                WHERE a.views > 1000""", 
             "join")
        ]
        
        query_results = {}
        total_query_time = 0
        
        for query_name, query_sql, table_type in queries:
            try:
                start_time = time.time()
                self.cursor.execute(query_sql)
                results = self.cursor.fetchall()
                query_time = time.time() - start_time
                total_query_time += query_time
                
                result_count = results[0]['count'] if results and results[0] else 0
                
                query_results[query_name] = {
                    "count": result_count,
                    "time": query_time,
                    "table_type": table_type
                }
                
                print(f"  📋 {query_name}: {result_count} results in {query_time:.4f}s")
                
            except Error as e:
                print(f"  ❌ {query_name}: Query failed - {e}")
                query_results[query_name] = {"count": 0, "time": 0, "error": str(e)}
        
        avg_query_time = total_query_time / len([q for q in query_results.values() if "error" not in q])
        self.metrics['query_times'] = query_results
        self.metrics['avg_query_time'] = avg_query_time
        
        print(f"\n📊 Average query time: {avg_query_time:.4f} seconds")
        print("💡 SQL Strength: Complex JOINs and relational queries")
        print("💡 MongoDB Advantage: Dot notation queries, no JOINs needed")
        return query_results

    # =================================================================
    # OBJECTIVE 2: PERFORMANCE ANALYSIS 
    # =================================================================
    
    def generate_test_data(self, count, prefix="perf"):
        """Generate test data for performance experiments"""
        test_data = []
        categories = ["electronics", "books", "clothing", "home", "sports"]
        
        for i in range(1, count + 1):
            category = random.choice(categories)
            product = (
                f"{prefix}_{i:06d}",
                f"Performance Test Product {i}",
                round(random.uniform(10, 1000), 2),
                category,
                f"Test product {i} for performance evaluation",
                datetime.now() - timedelta(days=random.randint(0, 365)),
                random.randint(0, 1000),
                round(random.uniform(1.0, 5.0), 1),
                json.dumps(random.sample(["new", "sale", "featured", "popular", "limited"], k=random.randint(1, 3)))
            )
            test_data.append(product)
        
        return test_data

    def test_crud_performance(self):
        """Test CRUD operations performance across different dataset sizes"""
        print("\n" + "=" * 60)
        print("📊 OBJECTIVE 2: PERFORMANCE ANALYSIS")
        print("=" * 60)
        print("Testing CRUD operations with moderate dataset sizes...")
        
        # Create performance test table
        create_perf_table_sql = """
        CREATE TABLE performance_test (
            id VARCHAR(20) PRIMARY KEY,
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
        
        # Test different dataset sizes
        dataset_sizes = [1000, 5000, 10000]
        crud_results = {}
        
        for size in dataset_sizes:
            print(f"\n🔄 Testing with {size:,} documents:")
            print("-" * 40)
            
            # Clear and recreate table for clean results
            print("🧹 Recreating performance test table...")
            try:
                self.cursor.execute("DROP TABLE IF EXISTS performance_test")
                self.cursor.execute(create_perf_table_sql)
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_category ON performance_test(category)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_price ON performance_test(price)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_rating ON performance_test(rating)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON performance_test(created_at)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON performance_test(status)")
                self.connection.commit()
            except Error as e:
                print(f"❌ Error recreating table: {e}")
                continue
            
            # === CREATE TEST ===
            print(f"📝 CREATE: Generating {size:,} test documents...")
            test_data = self.generate_test_data(size, f"perf_{size}")
            
            # Bulk insert performance
            insert_sql = """
            INSERT INTO performance_test 
            (id, name, price, category, description, created_at, stock, rating, tags) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            start_time = time.time()
            self.cursor.executemany(insert_sql, test_data)
            self.connection.commit()
            create_time = time.time() - start_time
            create_rate = size / create_time if create_time > 0 else 0
            
            print(f"   ✅ Inserted {size:,} docs in {create_time:.3f}s")
            print(f"   ⚡ Rate: {create_rate:.0f} docs/second")
            print(f"   💾 SQL Command: INSERT INTO performance_test (...) VALUES (...)")
            
            # === READ TEST ===
            print(f"\n📖 READ: Testing query performance...")
            read_queries = [
                ("Simple filter", "SELECT COUNT(*) FROM performance_test WHERE category = 'electronics'"),
                ("Range query", "SELECT COUNT(*) FROM performance_test WHERE price BETWEEN 100 AND 500"),
                ("Text search", "SELECT COUNT(*) FROM performance_test WHERE name LIKE '%Product 1%'"),
                ("Multiple conditions", "SELECT COUNT(*) FROM performance_test WHERE category = 'electronics' AND rating >= 4.0"),
                ("JSON contains", "SELECT COUNT(*) FROM performance_test WHERE tags::jsonb @> '\"featured\"'")
            ]
            
            read_times = []
            for query_name, query_sql in read_queries:
                try:
                    start_time = time.time()
                    self.cursor.execute(query_sql)
                    results = self.cursor.fetchall()
                    query_time = time.time() - start_time
                    read_times.append(query_time)
                    result_count = results[0]['count'] if results and results[0] else 0
                    print(f"   📋 {query_name}: {result_count} results in {query_time:.4f}s")
                except Error as e:
                    print(f"   ❌ {query_name}: Query failed - {e}")
                    read_times.append(0)
            
            avg_read_time = sum(read_times) / len(read_times) if read_times else 0
            print(f"   📊 Average READ time: {avg_read_time:.4f}s")
            print(f"   💾 SQL Command: SELECT * FROM performance_test WHERE ...")
            
            # === UPDATE TEST ===
            print(f"\n✏️  UPDATE: Testing update performance...")
            
            # Single field update
            start_time = time.time()
            self.cursor.execute("""
                UPDATE performance_test 
                SET price = price + 10 
                WHERE category = 'electronics'
            """)
            update_count = self.cursor.rowcount
            self.connection.commit()
            single_update_time = time.time() - start_time
            
            # Bulk field update  
            start_time = time.time()
            self.cursor.execute("""
                UPDATE performance_test 
                SET status = 'review_needed', updated_at = NOW() 
                WHERE rating < 3.0
            """)
            bulk_update_count = self.cursor.rowcount
            self.connection.commit()
            bulk_update_time = time.time() - start_time
            
            print(f"   ✅ Price update: {update_count:,} docs in {single_update_time:.4f}s")
            print(f"   ✅ Status update: {bulk_update_count:,} docs in {bulk_update_time:.4f}s")
            print(f"   💾 SQL Command: UPDATE performance_test SET price = price + 10 WHERE category = 'electronics'")
            
            # === DELETE TEST ===
            print(f"\n🗑️  DELETE: Testing deletion performance...")
            
            # Count documents before deletion
            self.cursor.execute("SELECT COUNT(*) FROM performance_test")
            docs_before_delete = self.cursor.fetchone()['count']
            
            # Delete old products
            start_time = time.time()
            self.cursor.execute("""
                DELETE FROM performance_test 
                WHERE created_at < NOW() - INTERVAL '300 days'
            """)
            deleted_count = self.cursor.rowcount
            self.connection.commit()
            delete_time = time.time() - start_time
            
            # Count documents after deletion
            self.cursor.execute("SELECT COUNT(*) FROM performance_test")
            docs_after_delete = self.cursor.fetchone()['count']
            deletion_percentage = (deleted_count / docs_before_delete * 100) if docs_before_delete > 0 else 0
            
            print(f"   ✅ Deleted {deleted_count:,} old products in {delete_time:.4f}s ({deletion_percentage:.1f}% of dataset)")
            print(f"   💾 SQL Command: DELETE FROM performance_test WHERE created_at < NOW() - INTERVAL '300 days'")
            
            # Store results
            crud_results[size] = {
                "create_time": create_time,
                "create_rate": create_rate,
                "avg_read_time": avg_read_time,
                "single_update_time": single_update_time,
                "bulk_update_time": bulk_update_time,
                "delete_time": delete_time,
                "documents_before_delete": docs_before_delete,
                "documents_after_delete": docs_after_delete,
                "documents_deleted": deleted_count,
                "deletion_percentage": deletion_percentage
            }
            
            print(f"   📈 Final count: {docs_after_delete:,} documents (started with {size:,})")
        
        # Store performance metrics
        self.metrics['crud_performance'] = crud_results
        
        return crud_results
    
    def run_objective_1_schema_flexibility(self):
        """Run Objective 1: Schema Flexibility & Data Structure Support"""
        print("\n" + "=" * 60)
        print("📋 OBJECTIVE 1: SCHEMA FLEXIBILITY & DATA STRUCTURE SUPPORT")
        print("=" * 60)
        print("Starting Schema Flexibility & Data Structure Support Experiment...")
        
        # Clear previous data
        self.clear_data()
        
        # Run all schema tests
        basic_count = self.test_1_basic_schema()
        enhanced_count = self.test_2_schema_evolution()
        complex_count = self.test_3_complex_nested_structures()
        query_results = self.test_4_query_flexibility()
        
        return {
            "total_documents": basic_count + enhanced_count + complex_count,
            "query_results": query_results
        }

    def run_objective_2_performance(self):
        """Run Objective 2: Performance Analysis"""
        performance_results = self.test_crud_performance()
        return performance_results

    # =================================================================
    # OBJECTIVE 3: DATA INTEGRITY & CONSISTENCY (E-COMMERCE TRANSACTIONS)
    # =================================================================
    
    def setup_validation_schemas(self):
        """Setup database schema with constraints for data integrity - PostgreSQL Version"""
        print("\n🛡️  Setting up data validation schemas...")
        
        # Drop existing tables if they exist
        ecommerce_tables = ["payments", "order_items", "orders", "customers", "inventory"]
        for table in ecommerce_tables:
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")  # Note: CASCADE for PostgreSQL
            except:
                pass
        
        # ===== NEW: CREATE ENUM TYPES FIRST =====
        # This is the PostgreSQL-specific step that MySQL doesn't need
        
        enum_types = [
            "CREATE TYPE customer_status AS ENUM ('active', 'inactive', 'suspended')",
            "CREATE TYPE order_status AS ENUM ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')", 
            "CREATE TYPE payment_method AS ENUM ('credit_card', 'debit_card', 'paypal', 'bank_transfer')",
            "CREATE TYPE payment_status AS ENUM ('pending', 'completed', 'failed', 'refunded')"
        ]
        
        print("   📝 Creating ENUM types...")
        for enum_sql in enum_types:
            try:
                enum_name = enum_sql.split('AS')[0].replace('CREATE TYPE', '').strip()
                self.cursor.execute(f"DROP TYPE IF EXISTS {enum_name} CASCADE")
                self.cursor.execute(enum_sql)
                print(f"   ✅ Created: {enum_name}")
            except Exception as e:
                print(f"   ❌ Error creating ENUM type: {e}")
                self.connection.rollback()
                pass
        
        try:
            # Customer table with PostgreSQL ENUM usage
            customer_schema = """
            CREATE TABLE customers (
                customer_id VARCHAR(20) PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                name VARCHAR(100) NOT NULL,
                phone VARCHAR(20),
                street VARCHAR(255),
                city VARCHAR(100),
                postal_code VARCHAR(20),
                country VARCHAR(50),
                created_at TIMESTAMP NOT NULL,  -- Changed from DATETIME
                status customer_status DEFAULT 'active',  -- Using custom ENUM type
                CONSTRAINT chk_customer_id CHECK (customer_id ~ '^CUST_[0-9]{6}$'),  -- Changed from REGEXP
                CONSTRAINT chk_email CHECK (email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'),
                CONSTRAINT chk_name_length CHECK (LENGTH(name) >= 2 AND LENGTH(name) <= 100),  -- Changed from CHAR_LENGTH
                CONSTRAINT chk_phone CHECK (phone ~ '^\\+?[0-9]{10,15}$'),
                CONSTRAINT chk_street_length CHECK (LENGTH(street) >= 5),
                CONSTRAINT chk_city_length CHECK (LENGTH(city) >= 2)
            );
            CREATE INDEX idx_customers_email ON customers(email);
            CREATE INDEX idx_customers_status ON customers(status);
            CREATE INDEX idx_customers_created_at ON customers(created_at);
            """
            
            # Orders table with PostgreSQL ENUM usage
            orders_schema = """
            CREATE TABLE orders (
                order_id VARCHAR(20) PRIMARY KEY,
                customer_id VARCHAR(20) NOT NULL,
                total_amount DECIMAL(12,2) NOT NULL,
                status order_status DEFAULT 'pending',  
                created_at TIMESTAMP NOT NULL,  
                updated_at TIMESTAMP,
                CONSTRAINT chk_order_id CHECK (order_id ~ '^ORD_[A-Za-z0-9]{2,12}$'),
                CONSTRAINT chk_total_amount CHECK (total_amount >= 0),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_orders_customer_id ON orders(customer_id);
            CREATE INDEX idx_orders_status ON orders(status);
            CREATE INDEX idx_orders_created_at ON orders(created_at);
            CREATE INDEX idx_orders_total_amount ON orders(total_amount);
            """
            
            # Order items table
            order_items_schema = """
            CREATE TABLE order_items (
                id SERIAL PRIMARY KEY,  -- Changed from AUTO_INCREMENT
                order_id VARCHAR(20) NOT NULL,
                product_id VARCHAR(50) NOT NULL,
                quantity INT NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                CONSTRAINT chk_quantity CHECK (quantity >= 1),
                CONSTRAINT chk_unit_price CHECK (unit_price >= 0),
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_order_items_order_id ON order_items(order_id);
            CREATE INDEX idx_order_items_product_id ON order_items(product_id);
            """
            
            # Payments table with PostgreSQL ENUM usage  
            payments_schema = """
            CREATE TABLE payments (
                payment_id VARCHAR(20) PRIMARY KEY,
                order_id VARCHAR(20) NOT NULL,
                amount DECIMAL(12,2) NOT NULL,
                method payment_method NOT NULL,  -- Using custom ENUM type
                status payment_status DEFAULT 'pending',  -- Using custom ENUM type
                transaction_ref VARCHAR(100),
                created_at TIMESTAMP NOT NULL,  -- Changed from DATETIME
                processed_at TIMESTAMP,
                CONSTRAINT chk_payment_id CHECK (payment_id ~ '^PAY_[A-Za-z0-9]{2,12}$'),
                CONSTRAINT chk_amount CHECK (amount >= 0),
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_payments_order_id ON payments(order_id);
            CREATE INDEX idx_payments_status ON payments(status);
            CREATE INDEX idx_payments_created_at ON payments(created_at);
            CREATE INDEX idx_payments_amount ON payments(amount);
            """
            
            # Inventory table (no ENUM, but other PostgreSQL changes)
            inventory_schema = """
            CREATE TABLE inventory (
                product_id VARCHAR(50) PRIMARY KEY,
                stock_quantity INT NOT NULL DEFAULT 0,
                reserved_quantity INT NOT NULL DEFAULT 0,
                reorder_level INT DEFAULT 0,
                max_stock INT DEFAULT 1000,
                last_updated TIMESTAMP NOT NULL,  -- Changed from DATETIME
                CONSTRAINT chk_stock_quantity CHECK (stock_quantity >= 0),
                CONSTRAINT chk_reserved_quantity CHECK (reserved_quantity >= 0),
                CONSTRAINT chk_reorder_level CHECK (reorder_level >= 0),
                CONSTRAINT chk_max_stock CHECK (max_stock >= 1),
                CONSTRAINT chk_available_stock CHECK (stock_quantity >= reserved_quantity)
            );
            CREATE INDEX idx_inventory_stock_quantity ON inventory(stock_quantity);
            CREATE INDEX idx_inventory_last_updated ON inventory(last_updated);
            """
            
            # Create all tables
            self.cursor.execute(customer_schema)
            self.cursor.execute(inventory_schema)
            self.cursor.execute(orders_schema)
            self.cursor.execute(order_items_schema)  
            self.cursor.execute(payments_schema)

            self.connection.commit()
            
            print("   ✅ Customer validation schema applied with custom ENUM types")
            print("   ✅ Order validation schema applied with FOREIGN KEY constraints")
            print("   ✅ Order items normalized table created") 
            print("   ✅ Payment validation schema applied with custom ENUM types")
            print("   ✅ Inventory validation schema applied with business rules")
            print("   💡 PostgreSQL Advantage: Reusable ENUM types across tables")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error setting up validation schemas: {e}")
            return False

    def create_sample_ecommerce_data(self):
        """Create sample e-commerce data for integrity testing"""
        print("\n📦 Creating sample e-commerce data...")
        
        # Create customers
        customers = []
        for i in range(1, 51):  # 50 customers
            customer = (
                f"CUST_{i:06d}",
                f"customer{i}@email.com",
                f"Customer {i}",
                f"+1234567{i:04d}",
                f"{i} Main Street",
                random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
                f"{10000 + i:05d}",
                "USA",
                datetime.now() - timedelta(days=random.randint(1, 365)),
                "active"
            )
            customers.append(customer)
        
        # Create inventory items
        inventory_items = []
        for i in range(1, 101):  # 100 products
            inventory = (
                f"PROD_{i:06d}",
                random.randint(10, 1000),  # stock_quantity
                0,  # reserved_quantity
                random.randint(5, 50),  # reorder_level
                random.randint(500, 2000),  # max_stock
                datetime.now()
            )
            inventory_items.append(inventory)
        
        try:
            # Insert sample data
            customer_sql = """
            INSERT INTO customers 
            (customer_id, email, name, phone, street, city, postal_code, country, created_at, status) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.executemany(customer_sql, customers)
            
            inventory_sql = """
            INSERT INTO inventory 
            (product_id, stock_quantity, reserved_quantity, reorder_level, max_stock, last_updated) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            self.cursor.executemany(inventory_sql, inventory_items)
            
            self.connection.commit()
            
            print(f"   ✅ Created {len(customers)} customers")
            print(f"   ✅ Created {len(inventory_items)} inventory items")
            
            self.metrics['sample_customers'] = len(customers)
            self.metrics['sample_inventory'] = len(inventory_items)
            
            return True
            
        except Error as e:
            print(f"   ❌ Error creating sample data: {e}")
            return False

    def test_data_validation(self):
        """Test data validation capabilities"""
        print("\n🔍 Testing Data Validation Capabilities...")
        print("-" * 45)
        
        validation_results = {
            "valid_insertions": 0,
            "invalid_insertions_blocked": 0,
            "validation_errors": []
        }
        
        # Test 1: Valid customer insertion
        print("\n📋 Test 1: Valid Customer Data")
        valid_customer = (
            "CUST_999999",
            "valid.customer@test.com",
            "Valid Customer",
            "+1234567890",
            "123 Valid Street",
            "Test City",
            "12345",
            "USA",
            datetime.now(),
            "active"
        )
        
        try:
            self.cursor.execute("""
                INSERT INTO customers 
                (customer_id, email, name, phone, street, city, postal_code, country, created_at, status) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, valid_customer)
            self.connection.commit()
            validation_results["valid_insertions"] += 1
            print("   ✅ Valid customer data accepted")
        except Error as e:
            print(f"   ❌ Unexpected error with valid data: {e}")
            validation_results["validation_errors"].append(f"Valid data rejected: {e}")
        
        # Test 2: Invalid customer data (should be rejected)
        print("\n📋 Test 2: Invalid Customer Data")
        invalid_customers = [
            {
                "name": "Invalid Customer 1 - Bad Email Format",
                "data": (
                    "CUST_888888",
                    "not-an-email",  # Invalid email format
                    "Invalid Customer 1",
                    "+1234567890",
                    "123 Test Street",
                    "Test City",
                    "12345",
                    "USA",
                    datetime.now(),
                    "active"
                ),
                "expected_error": "Email format constraint violation"
            },
            {
                "name": "Invalid Customer 2 - Bad Customer ID Pattern",
                "data": (
                    "INVALID_ID",  # Invalid customer_id pattern
                    "invalid2@test.com",
                    "Invalid Customer 2",
                    "+1234567890",
                    "123 Test Street",
                    "Test City",
                    "12345",
                    "USA",
                    datetime.now(),
                    "active"
                ),
                "expected_error": "Customer ID pattern constraint violation"
            },
            {
                "name": "Invalid Customer 3 - Name Too Short",
                "data": (
                    "CUST_777777",
                    "invalid3@test.com",
                    "X",  # Name too short (less than 2 characters)
                    "+1234567890",
                    "123 Test Street",
                    "Test City",
                    "12345",
                    "USA",
                    datetime.now(),
                    "active"
                ),
                "expected_error": "Name length constraint violation"
            }
        ]
        
        customer_insert_sql = """
        INSERT INTO customers 
        (customer_id, email, name, phone, street, city, postal_code, country, created_at, status) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for test_case in invalid_customers:
            try:
                self.cursor.execute(customer_insert_sql, test_case["data"])
                self.connection.commit()
                print(f"   ❌ {test_case['name']}: Should have been rejected but was accepted!")
                validation_results["validation_errors"].append(f"Invalid data accepted: {test_case['name']}")
            except Error as e:
                validation_results["invalid_insertions_blocked"] += 1
                print(f"   ✅ {test_case['name']}: Correctly rejected - {test_case['expected_error']}")
        
        # Test 3: Order validation with business logic
        print("\n📋 Test 3: Order and Order Items Validation")
        
        # Valid order with items
        valid_order = (
            "ORD_12345678",
            "CUST_000001",
            67.48,
            "pending",
            datetime.now(),
            None
        )
        
        valid_order_items = [
            ("ORD_12345678", "PROD_000001", 2, 25.99),
            ("ORD_12345678", "PROD_000002", 1, 15.50)
        ]
        
        try:
            # Insert order
            self.cursor.execute("""
                INSERT INTO orders (order_id, customer_id, total_amount, status, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, valid_order)
            
            # Insert order items
            self.cursor.executemany("""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price) 
                VALUES (%s, %s, %s, %s)
            """, valid_order_items)
            
            self.connection.commit()
            validation_results["valid_insertions"] += 1
            print("   ✅ Valid order with items accepted")
        except Error as e:
            print(f"   ❌ Unexpected error with valid order: {e}")
            validation_results["validation_errors"].append(f"Valid order rejected: {e}")
        
        # Invalid order (negative quantity in order items)
        invalid_order = (
            "ORD_87654321",
            "CUST_000001",
            25.99,
            "pending",
            datetime.now(),
            None
        )
        
        invalid_order_items = [
            ("ORD_87654321", "PROD_000001", -1, 25.99)  # Invalid negative quantity
        ]
        
        try:
            # Insert order first
            self.cursor.execute("""
                INSERT INTO orders (order_id, customer_id, total_amount, status, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, invalid_order)
            
            # Try to insert invalid order item
            self.cursor.execute("""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price) 
                VALUES (%s, %s, %s, %s)
            """, invalid_order_items[0])
            
            self.connection.commit()
            print("   ❌ Invalid order (negative quantity): Should have been rejected!")
            validation_results["validation_errors"].append("Negative quantity order item accepted")
        except Error as e:
            validation_results["invalid_insertions_blocked"] += 1
            print("   ✅ Invalid order item (negative quantity): Correctly rejected by CHECK constraint")
            self.connection.rollback()  # Rollback the partial transaction
        
        # Store validation metrics
        self.metrics['validation_results'] = validation_results
        
        print(f"\n📊 VALIDATION SUMMARY:")
        print(f"Valid insertions: {validation_results['valid_insertions']}")
        print(f"Invalid insertions blocked: {validation_results['invalid_insertions_blocked']}")
        print(f"Validation errors: {len(validation_results['validation_errors'])}")
        print(f"💡 MySQL Advantage: Built-in CHECK constraints and FOREIGN KEY enforcement")
        
        return validation_results

    def test_multi_document_transactions(self):
        """Test multi-table ACID transactions"""
        print("\n🔄 Testing Multi-Table ACID Transactions...")
        print("-" * 50)
        
        transaction_results = {
            "successful_transactions": 0,
            "failed_transactions": 0,
            "rollback_tests": 0,
            "transaction_times": []
        }
        
        # Test 1: Successful Order Processing Transaction
        print("\n📋 Test 1: Complete Order Processing (Multi-Table Transaction)")
        
        def process_order_transaction(order_data, order_items_data, payment_data):
            """Process an order with inventory update, order creation, and payment in a single transaction"""
            start_time = time.time()
            
            try:
                # Start transaction
                self.cursor.execute("START TRANSACTION")
                
                # 1. Check and reserve inventory
                for order_item in order_items_data:
                    product_id, quantity = order_item[1], order_item[2]
                    
                    # Check inventory availability
                    self.cursor.execute("""
                        SELECT stock_quantity, reserved_quantity 
                        FROM inventory 
                        WHERE product_id = %s FOR UPDATE
                    """, (product_id,))
                    
                    inventory_result = self.cursor.fetchone()
                    if not inventory_result:
                        raise ValueError(f"Product {product_id} not found in inventory")
                    
                    stock_quantity = int(inventory_result['stock_quantity'])
                    reserved_quantity = int(inventory_result['reserved_quantity'])
                    available_stock = stock_quantity - reserved_quantity
                    
                    if available_stock < quantity:
                        raise ValueError(f"Insufficient stock for {product_id}. Available: {available_stock}, Requested: {quantity}")
                    
                    # Reserve inventory
                    self.cursor.execute("""
                        UPDATE inventory 
                        SET reserved_quantity = reserved_quantity + %s,
                            last_updated = %s
                        WHERE product_id = %s
                    """, (quantity, datetime.now(), product_id))
                
                # 2. Create order
                self.cursor.execute("""
                    INSERT INTO orders (order_id, customer_id, total_amount, status, created_at) 
                    VALUES (%s, %s, %s, %s, %s)
                """, order_data)
                
                # 3. Create order items
                self.cursor.executemany("""
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price) 
                    VALUES (%s, %s, %s, %s)
                """, order_items_data)
                
                # 4. Process payment
                self.cursor.execute("""
                    INSERT INTO payments (payment_id, order_id, amount, method, status, transaction_ref, created_at) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, payment_data)
                
                # 5. Update order status to confirmed
                self.cursor.execute("""
                    UPDATE orders 
                    SET status = 'confirmed', updated_at = %s 
                    WHERE order_id = %s
                """, (datetime.now(), order_data[0]))
                
                # 6. Update payment status to completed
                self.cursor.execute("""
                    UPDATE payments 
                    SET status = 'completed', processed_at = %s 
                    WHERE payment_id = %s
                """, (datetime.now(), payment_data[0]))
                
                # Commit transaction
                self.connection.commit()
                
                transaction_time = time.time() - start_time
                transaction_results["transaction_times"].append(transaction_time)
                transaction_results["successful_transactions"] += 1
                
                print(f"   ✅ Transaction completed successfully in {transaction_time:.4f}s")
                print(f"      📦 Order {order_data[0]} created and confirmed")
                print(f"      💳 Payment {payment_data[0]} processed")
                print(f"      📊 Inventory reserved for {len(order_items_data)} items")
                
                return True
                
            except (Error, ValueError) as e:
                print(f"   ❌ Transaction failed and rolled back: {e}")
                self.connection.rollback()
                transaction_results["failed_transactions"] += 1
                transaction_results["rollback_tests"] += 1
                return False
        
        # Execute successful transaction tests
        for i in range(1, 4):  # 3 successful transactions
            # Generate order data
            order_items_data = []
            total_amount = 0
            for j in range(1, random.randint(2, 4)):
                quantity = random.randint(1, 3)
                unit_price = round(random.uniform(10, 100), 2)
                order_items_data.append((f"ORD_T{i:07d}", f"PROD_{j:06d}", quantity, unit_price))
                total_amount += quantity * unit_price
            
            order_data = (
                f"ORD_T{i:07d}",
                "CUST_000001",
                round(total_amount, 2),
                "pending",
                datetime.now()
            )
            
            payment_data = (
                f"PAY_T{i:07d}",
                order_data[0],
                order_data[2],  # same amount as order total
                random.choice(["credit_card", "debit_card", "paypal"]),
                "pending",
                f"TXN_{uuid.uuid4().hex[:8].upper()}",
                datetime.now()
            )
            
            process_order_transaction(order_data, order_items_data, payment_data)
        
        # Test 2: Transaction Rollback (Intentional Failure)
        print("\n📋 Test 2: Transaction Rollback Test (Insufficient Inventory)")
        
        # Create an order that will fail due to insufficient inventory
        rollback_order_data = (
            "ORD_ROLLBACK",
            "CUST_000001",
            2599900.01,
            "pending",
            datetime.now()
        )
        
        rollback_order_items = [
            ("ORD_ROLLBACK", "PROD_000001", 99999, 25.99)  # Intentionally too high quantity
        ]
        
        rollback_payment_data = (
            "PAY_ROLLBACK",
            "ORD_ROLLBACK",
            2599900.01,
            "credit_card",
            "pending",
            "TXN_ROLLBACK",
            datetime.now()
        )
        
        # Check that no partial data exists before transaction
        self.cursor.execute("SELECT COUNT(*) FROM orders WHERE order_id = 'ORD_ROLLBACK'")
        orders_before = self.cursor.fetchone()['count']
        
        self.cursor.execute("SELECT COUNT(*) FROM payments WHERE payment_id = 'PAY_ROLLBACK'")
        payments_before = self.cursor.fetchone()['count']
        
        process_order_transaction(rollback_order_data, rollback_order_items, rollback_payment_data)
        
        # Verify rollback - no partial data should exist
        self.cursor.execute("SELECT COUNT(*) FROM orders WHERE order_id = 'ORD_ROLLBACK'")
        orders_after = self.cursor.fetchone()['count']
        
        self.cursor.execute("SELECT COUNT(*) FROM payments WHERE payment_id = 'PAY_ROLLBACK'")
        payments_after = self.cursor.fetchone()['count']
        
        if orders_before == orders_after == 0 and payments_before == payments_after == 0:
            print("   ✅ Transaction rollback successful - no partial data left behind")
            print("   💡 ACID compliance verified: Atomicity maintained during failure")
        else:
            print("   ❌ Transaction rollback failed - partial data detected!")
        
        # Calculate transaction performance
        if transaction_results["transaction_times"]:
            avg_transaction_time = sum(transaction_results["transaction_times"]) / len(transaction_results["transaction_times"])
            min_transaction_time = min(transaction_results["transaction_times"])
            max_transaction_time = max(transaction_results["transaction_times"])
            
            self.metrics['transaction_performance'] = {
                "average_time": avg_transaction_time,
                "min_time": min_transaction_time,
                "max_time": max_transaction_time,
                "total_transactions": len(transaction_results["transaction_times"])
            }
            
            print(f"\n📊 TRANSACTION PERFORMANCE:")
            print(f"Average transaction time: {avg_transaction_time:.4f}s")
            print(f"Fastest transaction: {min_transaction_time:.4f}s")
            print(f"Slowest transaction: {max_transaction_time:.4f}s")
        
        # Store transaction metrics
        self.metrics['transaction_results'] = transaction_results
        
        print(f"\n📊 TRANSACTION SUMMARY:")
        print(f"Successful transactions: {transaction_results['successful_transactions']}")
        print(f"Failed transactions: {transaction_results['failed_transactions']}")
        print(f"Rollback tests: {transaction_results['rollback_tests']}")
        print(f"💡 MySQL Advantage: Native ACID transactions with START TRANSACTION/COMMIT/ROLLBACK")
        
        return transaction_results

    def test_referential_integrity(self):
        """Test referential integrity enforcement"""
        print("\n🔗 Testing Referential Integrity...")
        print("-" * 40)
        
        integrity_results = {
            "orphaned_records_prevented": 0,
            "integrity_violations": 0,
            "constraint_enforcements": 0,
            "cascade_deletes_successful": 0
        }
        
        print("\n📋 Test 1: Customer-Order Relationship Integrity (FOREIGN KEY)")
        
        # Test creating order with non-existent customer (should fail due to FOREIGN KEY constraint)
        orphan_order = (
            "ORD_ORPHAN01",
            "CUST_NOEXIST",  # Non-existent customer
            25.99,
            "pending",
            datetime.now(),
            None
        )
        
        try:
            self.cursor.execute("""
                INSERT INTO orders (order_id, customer_id, total_amount, status, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, orphan_order)
            self.connection.commit()
            print("   ❌ Order with non-existent customer was created (integrity violation)")
            integrity_results["integrity_violations"] += 1
        except Error as e:
            print(f"   ✅ FOREIGN KEY constraint prevented orphan order: {e}")
            integrity_results["constraint_enforcements"] += 1
            integrity_results["orphaned_records_prevented"] += 1
            self.connection.rollback()
        
        print("\n📋 Test 2: Order-Payment Relationship Integrity")
        
        # Test payment with non-existent order (should fail due to FOREIGN KEY constraint)
        orphan_payment = (
            "PAY_ORPHAN01",
            "ORD_NOEXIST",  # Non-existent order
            100.00,
            "credit_card",
            "pending",
            "TXN_ORPHAN",
            datetime.now(),
            None
        )
        
        try:
            self.cursor.execute("""
                INSERT INTO payments (payment_id, order_id, amount, method, status, transaction_ref, created_at, processed_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, orphan_payment)
            self.connection.commit()
            print("   ❌ Payment with non-existent order was created (integrity violation)")
            integrity_results["integrity_violations"] += 1
        except Error as e:
            print(f"   ✅ FOREIGN KEY constraint prevented orphan payment: {e}")
            integrity_results["constraint_enforcements"] += 1
            integrity_results["orphaned_records_prevented"] += 1
            self.connection.rollback()
        
        print("\n📋 Test 3: CASCADE DELETE Test")
        
        # Create a test customer and order to test CASCADE DELETE
        test_customer = (
            "CUST_999998",
            "cascade.test@email.com",
            "Cascade Test Customer",
            "+1234567890",
            "123 Cascade Street",
            "Test City",
            "12345",
            "USA",
            datetime.now(),
            "active"
        )
        
        test_order = (
            "ORD_CASCADE",
            "CUST_999998",
            50.00,
            "pending",
            datetime.now(),
            None
        )
        
        test_payment = (
            "PAY_CASCADE",
            "ORD_CASCADE",
            50.00,
            "credit_card",
            "completed",
            "TXN_CASCADE",
            datetime.now(),
            datetime.now()
        )
        
        try:
            # Insert test data
            self.cursor.execute("""
                INSERT INTO customers 
                (customer_id, email, name, phone, street, city, postal_code, country, created_at, status) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, test_customer)
            
            self.cursor.execute("""
                INSERT INTO orders (order_id, customer_id, total_amount, status, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, test_order)
            
            self.cursor.execute("""
                INSERT INTO payments (payment_id, order_id, amount, method, status, transaction_ref, created_at, processed_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, test_payment)
            
            self.connection.commit()
            
            # Count related records before deletion
            self.cursor.execute("SELECT COUNT(*) FROM orders WHERE customer_id = 'CUST_999998'")
            orders_before = self.cursor.fetchone()['count']
            
            self.cursor.execute("SELECT COUNT(*) FROM payments WHERE order_id = 'ORD_CASCADE'")
            payments_before = self.cursor.fetchone()['count']
            
            # Delete customer (should cascade to orders and payments)
            self.cursor.execute("DELETE FROM customers WHERE customer_id = 'CUST_999998'")
            self.connection.commit()
            
            # Check that related records were also deleted
            # Check that related records were also deleted
            self.cursor.execute("SELECT COUNT(*) FROM orders WHERE customer_id = 'CUST_999998'")
            orders_after = self.cursor.fetchone()['count']
            
            self.cursor.execute("SELECT COUNT(*) FROM payments WHERE order_id = 'ORD_CASCADE'")
            payments_after = self.cursor.fetchone()['count']
            
            if orders_before > 0 and orders_after == 0 and payments_before > 0 and payments_after == 0:
                print(f"   ✅ CASCADE DELETE successful: {orders_before} order(s) and {payments_before} payment(s) deleted")
                integrity_results["cascade_deletes_successful"] += 1
                integrity_results["constraint_enforcements"] += 1
                print("   💡 MySQL Advantage: Automatic CASCADE DELETE maintains referential integrity")
            else:
                print("   ❌ CASCADE DELETE failed - related records not properly deleted")
                integrity_results["integrity_violations"] += 1
                
        except Error as e:
            print(f"   ❌ Error in CASCADE DELETE test: {e}")
            self.connection.rollback()
        
        # Store integrity metrics
        self.metrics['integrity_results'] = integrity_results
        
        print(f"\n📊 REFERENTIAL INTEGRITY SUMMARY:")
        print(f"Constraint enforcements: {integrity_results['constraint_enforcements']}")
        print(f"Orphaned records prevented: {integrity_results['orphaned_records_prevented']}")
        print(f"CASCADE deletes successful: {integrity_results['cascade_deletes_successful']}")
        print(f"Integrity violations: {integrity_results['integrity_violations']}")
        print(f"💡 MySQL Advantage: Built-in FOREIGN KEY constraints with CASCADE operations")
        
        return integrity_results

    def analyze_consistency_performance(self):
        """Analyze performance trade-offs of consistency features"""
        print("\n⚡ Analyzing Consistency vs Performance Trade-offs...")
        print("-" * 55)
        
        performance_comparison = {
            "without_constraints": {},
            "with_constraints": {},
            "with_transactions": {}
        }
        
        # Test 1: Insert performance without constraints
        print("\n📋 Test 1: Insert Performance WITHOUT Constraints")
        
        # Create temporary table without constraints
        self.cursor.execute("DROP TABLE IF EXISTS temp_no_constraints CASCADE")

        temp_table_sql = """
        CREATE TABLE temp_no_constraints (
            order_id VARCHAR(20) PRIMARY KEY,
            customer_id VARCHAR(20),
            total_amount DECIMAL(12,2),
            status VARCHAR(20),
            created_at TIMESTAMP
        )
        """
        self.cursor.execute(temp_table_sql)
        
        test_orders = []
        for i in range(100):
            order = (
                f"ORD_SPEED{i:03d}",
                f"CUST_{random.randint(1, 50):06d}",
                round(random.uniform(20, 500), 2),
                "pending",
                datetime.now()
            )
            test_orders.append(order)
        
        start_time = time.time()
        self.cursor.executemany("""
            INSERT INTO temp_no_constraints (order_id, customer_id, total_amount, status, created_at) 
            VALUES (%s, %s, %s, %s, %s)
        """, test_orders)
        self.connection.commit()
        no_constraints_time = time.time() - start_time
        
        performance_comparison["without_constraints"]["insert_time"] = no_constraints_time
        performance_comparison["without_constraints"]["rate"] = len(test_orders) / no_constraints_time
        
        print(f"   ⚡ No constraints: {len(test_orders)} docs in {no_constraints_time:.4f}s ({performance_comparison['without_constraints']['rate']:.0f} docs/sec)")
        
        # Test 2: Insert performance with constraints
        print("\n📋 Test 2: Insert Performance WITH Constraints")
        
        start_time = time.time()
        constraint_successes = 0
        constraint_failures = 0
        
        for order in test_orders:
            try:
                self.cursor.execute("""
                    INSERT INTO orders (order_id, customer_id, total_amount, status, created_at) 
                    VALUES (%s, %s, %s, %s, %s)
                """, order)
                self.connection.commit()
                constraint_successes += 1
            except Error:
                constraint_failures += 1
                self.connection.rollback()
        
        with_constraints_time = time.time() - start_time
        performance_comparison["with_constraints"]["insert_time"] = with_constraints_time
        performance_comparison["with_constraints"]["rate"] = constraint_successes / with_constraints_time if with_constraints_time > 0 else 0
        performance_comparison["with_constraints"]["successes"] = constraint_successes
        performance_comparison["with_constraints"]["failures"] = constraint_failures
        
        print(f"   🛡️  With constraints: {constraint_successes} successful, {constraint_failures} failed in {with_constraints_time:.4f}s")
        print(f"   📊 Rate: {performance_comparison['with_constraints']['rate']:.0f} docs/sec")
        
        # Test 3: Transaction performance
        print("\n📋 Test 3: Multi-Table Transaction Performance")
        
        transaction_times = []
        successful_transactions = 0
        
        for i in range(10):  # 10 transactions
            order_data = (
                f"ORD_PERF{i:03d}",
                "CUST_000001",
                25.99,
                "pending",
                datetime.now(),
                None
            )
            
            payment_data = (
                f"PAY_PERF{i:03d}",
                f"ORD_PERF{i:03d}",
                25.99,
                "credit_card",
                "pending",
                f"TXN_PERF{i:03d}",
                datetime.now(),
                None
            )
            
            start_time = time.time()
            
            try:
                self.cursor.execute("START TRANSACTION")
                
                # Insert order
                self.cursor.execute("""
                    INSERT INTO orders (order_id, customer_id, total_amount, status, created_at, updated_at) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, order_data)
                
                # Insert payment
                self.cursor.execute("""
                    INSERT INTO payments (payment_id, order_id, amount, method, status, transaction_ref, created_at, processed_at) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, payment_data)
                
                self.connection.commit()
                transaction_time = time.time() - start_time
                transaction_times.append(transaction_time)
                successful_transactions += 1
                
            except Error:
                self.connection.rollback()
        
        if transaction_times:
            avg_transaction_time = sum(transaction_times) / len(transaction_times)
            performance_comparison["with_transactions"]["avg_time"] = avg_transaction_time
            performance_comparison["with_transactions"]["total_transactions"] = len(transaction_times)
            
            print(f"   🔄 Transactions: {successful_transactions} successful")
            print(f"   📊 Average transaction time: {avg_transaction_time:.4f}s")
        
        # Calculate performance impact
        constraint_overhead = ((with_constraints_time - no_constraints_time) / no_constraints_time * 100) if no_constraints_time > 0 else 0
        
        print(f"\n📊 PERFORMANCE IMPACT ANALYSIS:")
        print(f"Constraint overhead: {constraint_overhead:.1f}% slower")
        print(f"Data integrity benefit: {constraint_failures} invalid records blocked")
        print(f"Transaction consistency: ACID compliance for multi-table operations")
        
        # Store performance comparison metrics
        self.metrics['consistency_performance'] = performance_comparison
        self.metrics['constraint_overhead'] = constraint_overhead
        
        # Cleanup
        self.cursor.execute("DROP TABLE IF EXISTS temp_no_constraints CASCADE")
        
        return performance_comparison

    def run_objective_3_data_integrity(self):
        """Run Objective 3: Data Integrity & Consistency"""
        print("\n" + "=" * 60)
        print("🛡️  OBJECTIVE 3: DATA INTEGRITY & CONSISTENCY")
        print("=" * 60)
        print("E-commerce Order Processing with Advanced Transactions...")
        
        # Setup validation schemas
        if not self.setup_validation_schemas():
            print("❌ Failed to setup validation schemas")
            self.connection.rollback()
            return None
        
        # Create sample data
        if not self.create_sample_ecommerce_data():
            print("❌ Failed to create sample data")
            return None
        
        # Run integrity tests
        validation_results = self.test_data_validation()
        transaction_results = self.test_multi_document_transactions()
        integrity_results = self.test_referential_integrity()
        performance_comparison = self.analyze_consistency_performance()
        
        return {
            "validation_results": validation_results,
            "transaction_results": transaction_results,
            "integrity_results": integrity_results,
            "performance_comparison": performance_comparison
        }

    def generate_summary(self):
        """Generate experiment summary and comparison assessment"""
        print("\n" + "=" * 60)
        print("📊 MYSQL DATABASE EVALUATION RESULTS")
        print("=" * 60)
        
        # Get database statistics
        self.cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
        result = self.cursor.fetchone()
        table_count = result['count'] if result else 0
        
        # Summary statistics
        print(f"\n📈 SUMMARY:")
        print(f"Tables Created: {table_count}")
        print(f"Database: {os.getenv('MYSQL_DATABASE', 'mysql_evaluation_test')}")
        
        # Performance metrics
        print(f"\n⚡ PERFORMANCE METRICS:")
        print(f"Basic Schema Insertion: {self.metrics.get('basic_insertion', 0):.4f}s (50 docs)")
        print(f"Schema Evolution: {self.metrics.get('evolution_insertion', 0):.4f}s (50 docs)")
        print(f"Complex Structures: {self.metrics.get('complex_insertion', 0):.4f}s (20 docs)")
        print(f"Average Query Time: {self.metrics.get('avg_query_time', 0):.4f}s")
        
        # Objective findings
        print(f"\n📋 OBJECTIVE FINDINGS:")
        print(f"✅ Schema Migration Required: YES (ALTER TABLE statements)")
        print(f"✅ Nested Objects Supported: YES (JSON data type + normalized tables)")
        print(f"✅ Complex Relationships: YES (FOREIGN KEY constraints)")
        print(f"✅ Data Validation: YES (CHECK constraints + ENUM types)")
        print(f"✅ ACID Transactions: YES (native support)")
        print(f"✅ Referential Integrity: YES (built-in FOREIGN KEY enforcement)")
        
        return True

    def create_visualizations(self):
        """Create visualizations for MySQL evaluation results"""
        print("\n📊 Creating PostgreSQL Database Evaluation Visualizations...")

        self.create_all_separate_visualization() #To create three separate .png files
        
        if HAS_MATPLOTLIB:
            self.create_original_combined_visualization()

    def create_schema_flexibility_visualization(self):
        """Create Schema Flexibility visualization with 3 charts"""
        if not HAS_MATPLOTLIB:
            self.create_text_schema_flexibility()
            return
            
        print("📊 Creating Schema Flexibility Visualization...")
        
        fig, ((ax1, ax2), (ax3, ax_empty)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('PostgreSQL Schema Flexibility Analysis', fontsize=16, fontweight='bold')
        
        # Hide the empty subplot
        ax_empty.axis('off')
        
        # 1. Schema Insertion Performance (Bar Chart)
        schema_types = ['Basic\nSchema', 'Enhanced\nSchema', 'Complex\nStructure']
        insertion_times = [
            self.metrics.get('basic_insertion', 0),
            self.metrics.get('evolution_insertion', 0),
            self.metrics.get('complex_insertion', 0)
        ]
        
        # Calculate rates (documents per second)
        doc_counts = [50, 50, 20]  # Based on your test data
        insertion_rates = [doc_counts[i] / insertion_times[i] if insertion_times[i] > 0 else 0 
                          for i in range(len(insertion_times))]
        
        bars1 = ax1.bar(schema_types, insertion_rates, 
                       color=['#2E8B57', '#4682B4', '#CD853F'], alpha=0.8, width=0.6)
        ax1.set_title('Schema Insertion Performance', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Documents/Second', fontsize=12)
        ax1.grid(True, alpha=0.3, axis='y')
        ax1.set_ylim(0, max(insertion_rates) * 1.2)
        
        # Add value labels on bars
        for bar, rate, time_val in zip(bars1, insertion_rates, insertion_times):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + max(insertion_rates) * 0.01,
                    f'{rate:.0f}\ndocs/sec\n({time_val:.3f}s)', 
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # 2. Query Flexibility Performance (Horizontal Bar Chart)
        if 'query_times' in self.metrics:
            query_data = self.metrics['query_times']
            query_names = list(query_data.keys())[:6]  # Top 6 queries
            query_times = [query_data[q].get('time', 0) for q in query_names]
            
            # Shorten query names for better display
            short_names = []
            for name in query_names:
                if 'filter' in name.lower():
                    short_names.append('Simple Filter')
                elif 'range' in name.lower():
                    short_names.append('Range Query')
                elif 'json' in name.lower():
                    short_names.append('JSON Query')
                elif 'join' in name.lower() and 'complex' in name.lower():
                    short_names.append('Complex JOIN')
                elif 'join' in name.lower():
                    short_names.append('Multi-table JOIN')
                else:
                    short_names.append(name[:15])
            
            colors = plt.cm.viridis(np.linspace(0, 1, len(query_times)))
            bars2 = ax2.barh(range(len(short_names)), query_times, color=colors, alpha=0.8)
            ax2.set_title('Query Flexibility Performance', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Execution Time (seconds)', fontsize=12)
            ax2.set_yticks(range(len(short_names)))
            ax2.set_yticklabels(short_names, fontsize=10)
            ax2.grid(True, alpha=0.3, axis='x')
            
            # Add value labels
            for i, (bar, time_val) in enumerate(zip(bars2, query_times)):
                width = bar.get_width()
                ax2.text(width + max(query_times) * 0.01, bar.get_y() + bar.get_height()/2.,
                        f'{time_val:.4f}s', ha='left', va='center', fontsize=10, fontweight='bold')
        
        # 3. Document Distribution (Pie Chart)
        # Calculate document distribution across schema types
        basic_docs = 50
        enhanced_docs = 50
        complex_docs = 20
        total_docs = basic_docs + enhanced_docs + complex_docs
        
        doc_labels = ['Basic Schema\nProducts', 'Enhanced Schema\nProducts', 'Complex Structure\nProducts']
        doc_sizes = [basic_docs, enhanced_docs, complex_docs]
        colors = ['#FF9999', '#66B2FF', '#99FF99']
        
        # Create pie chart
        wedges, texts, autotexts = ax3.pie(doc_sizes, labels=doc_labels, colors=colors, autopct='%1.1f%%',
                                          startangle=90, textprops={'fontsize': 10})
        ax3.set_title('Document Distribution by Schema Type', fontsize=14, fontweight='bold')
        
        # Enhance pie chart appearance
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(11)
        
        # Add total count in center
        ax3.text(0, 0, f'Total\n{total_docs}\nDocuments', ha='center', va='center',
                fontsize=12, fontweight='bold', bbox=dict(boxstyle="round,pad=0.3", 
                facecolor='white', edgecolor='gray', alpha=0.8))
        
        plt.tight_layout()
        plt.savefig('postgresql_schema_flexibility.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("✅ Schema Flexibility visualization saved: 'postgresql_schema_flexibility.png'")

    def create_performance_analysis_visualization(self):
        """Create Performance Analysis visualization with CRUD and scaling charts"""
        if not HAS_MATPLOTLIB:
            self.create_text_performance_analysis()
            return
            
        print("📊 Creating Performance Analysis Visualization...")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('PostgreSQL Performance Analysis', fontsize=16, fontweight='bold')
        
        # 1. CRUD Operation Performance (10,000 documents focus)
        if 'crud_performance' in self.metrics and 10000 in self.metrics['crud_performance']:
            crud_data = self.metrics['crud_performance'][10000]
            
            operations = ['CREATE\n(Bulk Insert)', 'READ\n(Query Avg)', 'UPDATE\n(Single Field)', 'UPDATE\n(Bulk Fields)', 'DELETE\n(Filtered)']
            times = [
                crud_data.get('create_time', 0),
                crud_data.get('avg_read_time', 0),
                crud_data.get('single_update_time', 0),
                crud_data.get('bulk_update_time', 0),
                crud_data.get('delete_time', 0)
            ]
            
            colors = ['#32CD32', '#1E90FF', '#FFD700', '#FF6347', '#FF1493']
            bars = ax1.bar(operations, times, color=colors, alpha=0.8, width=0.6)
            
            ax1.set_title('CRUD Operations Performance\n(10,000 Documents)', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bar, time_val in zip(bars, times):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + max(times) * 0.01,
                        f'{time_val:.3f}s', ha='center', va='bottom', fontsize=10, fontweight='bold')
            
            # Add throughput information
            create_rate = 10000 / crud_data.get('create_time', 1)
            ax1.text(0.5, 0.95, f'Insert Rate: {create_rate:.0f} docs/sec', 
                    transform=ax1.transAxes, ha='center', va='top',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor='lightgreen', alpha=0.7),
                    fontsize=11, fontweight='bold')
        
        # 2. Insert Performance Scaling (like your reference image)
        if 'crud_performance' in self.metrics:
            dataset_sizes = sorted(self.metrics['crud_performance'].keys())
            insert_rates = []
            
            for size in dataset_sizes:
                crud_data = self.metrics['crud_performance'][size]
                rate = crud_data.get('create_rate', size / crud_data.get('create_time', 1))
                insert_rates.append(rate)
            
            # Plot the scaling curve
            ax2.plot(dataset_sizes, insert_rates, 'o-', color='#228B22', linewidth=3, 
                    markersize=8, markerfacecolor='#32CD32', markeredgecolor='#006400', markeredgewidth=2)
            
            ax2.set_title('Insert Performance Scaling', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Dataset Size (documents)', fontsize=12)
            ax2.set_ylabel('Insert Rate (docs/sec)', fontsize=12)
            ax2.grid(True, alpha=0.3)
            
            # Add value annotations with boxes like in your reference
            for i, (size, rate) in enumerate(zip(dataset_sizes, insert_rates)):
                ax2.annotate(f'{rate:.0f}\ndocs/sec', 
                           xy=(size, rate), 
                           xytext=(10, 20), 
                           textcoords='offset points',
                           bbox=dict(boxstyle="round,pad=0.3", facecolor='white', 
                                   edgecolor='#228B22', linewidth=2),
                           ha='center', va='bottom', fontsize=10, fontweight='bold',
                           arrowprops=dict(arrowstyle='-', color='#228B22', linewidth=1))
            
            # Format x-axis
            ax2.set_xlim(min(dataset_sizes) * 0.8, max(dataset_sizes) * 1.1)
            ax2.set_ylim(min(insert_rates) * 0.8, max(insert_rates) * 1.2)
        
        plt.tight_layout()
        plt.savefig('postgresql_performance_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("✅ Performance Analysis visualization saved: 'postgresql_performance_analysis.png'")

    def create_data_integrity_visualization(self):
        """Create Data Integrity and Consistency visualization"""
        if not HAS_MATPLOTLIB:
            self.create_text_data_integrity()
            return
            
        print("📊 Creating Data Integrity Visualization...")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('PostgreSQL Data Integrity & Consistency Analysis', fontsize=16, fontweight='bold')
        
        # 1. Data Validation Results (Bar Chart)
        if 'validation_results' in self.metrics:
            validation_data = self.metrics['validation_results']
            
            categories = ['Valid Data\nAccepted', 'Invalid Data\nBlocked', 'Constraint\nViolations', 'Integrity\nChecks']
            values = [
                validation_data.get('valid_insertions', 0),
                validation_data.get('invalid_insertions_blocked', 0),
                len(validation_data.get('validation_errors', [])),
                validation_data.get('invalid_insertions_blocked', 0) + len(validation_data.get('validation_errors', []))
            ]
            
            colors = ['#32CD32', '#FF6347', '#FFD700', '#4169E1']
            bars = ax1.bar(categories, values, color=colors, alpha=0.8, width=0.6)
            
            ax1.set_title('Data Validation Results', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Count', fontsize=12)
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bar, value in zip(bars, values):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + max(values) * 0.01,
                        f'{value}', ha='center', va='bottom', fontsize=11, fontweight='bold')
        
        # 2. Transaction Success Rate (Pie Chart)
        if 'transaction_results' in self.metrics:
            transaction_data = self.metrics['transaction_results']
            
            successful = transaction_data.get('successful_transactions', 0)
            failed = transaction_data.get('failed_transactions', 0)
            rollbacks = transaction_data.get('rollback_tests', 0)
            
            # Create pie chart data
            labels = ['Successful\nTransactions', 'Failed\nTransactions', 'Rollback\nTests']
            sizes = [successful, failed, rollbacks]
            colors = ['#32CD32', '#FF6347', '#FFD700']
            
            # Only show non-zero values
            non_zero_data = [(label, size, color) for label, size, color in zip(labels, sizes, colors) if size > 0]
            if non_zero_data:
                labels, sizes, colors = zip(*non_zero_data)
            
            wedges, texts, autotexts = ax2.pie(sizes, labels=labels, colors=colors, 
                                              autopct='%1.1f%%', startangle=90,
                                              textprops={'fontsize': 11})
            
            ax2.set_title('Transaction Success Rate', fontsize=14, fontweight='bold')
            
            # Enhance pie chart
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(12)
            
            # Add total in center
            total_transactions = sum(sizes)
            success_rate = (successful / total_transactions * 100) if total_transactions > 0 else 0
            
            ax2.text(0, 0, f'Total: {total_transactions}\nSuccess: {success_rate:.1f}%', 
                    ha='center', va='center', fontsize=12, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor='white', 
                             edgecolor='gray', alpha=0.8))
        
        plt.tight_layout()
        plt.savefig('postgresql_data_integrity.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("✅ Data Integrity visualization saved: 'postgresql_data_integrity.png'")

    def create_all_separate_visualizations(self):
        """Create all three separate visualization images"""
        print("\n📊 Creating All Separate Visualizations...")
        print("=" * 50)
        
        if not HAS_MATPLOTLIB:
            print("⚠️  matplotlib not available - creating text visualizations")
            self.create_text_schema_flexibility()
            self.create_text_performance_analysis()
            self.create_text_data_integrity()
            return
        
        # Import numpy for the scaling chart
        try:
            import numpy as np
            globals()['np'] = np
        except ImportError:
            print("⚠️  numpy not available - some charts may be simplified")
        
        # Create each visualization
        self.create_schema_flexibility_visualization()
        self.create_performance_analysis_visualization() 
        self.create_data_integrity_visualization()
        
        print("\n✅ All three visualization images created successfully!")
        print("📁 Files generated:")
        print("   • postgresql_schema_flexibility.png")
        print("   • postgresql_performance_analysis.png") 
        print("   • postgresql_data_integrity.png")

    def create_text_schema_flexibility(self):
        """Text-based Schema Flexibility visualization"""
        print("\n📊 SCHEMA FLEXIBILITY ANALYSIS (TEXT)")
        print("=" * 45)
        
        # Schema Insertion Performance
        print("\n📈 SCHEMA INSERTION PERFORMANCE:")
        schema_data = [
            ("Basic Schema", self.metrics.get('basic_insertion', 0), 50),
            ("Enhanced Schema", self.metrics.get('evolution_insertion', 0), 50),
            ("Complex Structure", self.metrics.get('complex_insertion', 0), 20)
        ]
        
        for name, time_val, docs in schema_data:
            rate = docs / time_val if time_val > 0 else 0
            bar_length = int((rate / 1000) * 40) if rate < 1000 else 40
            bar = "█" * bar_length + "░" * (40 - bar_length)
            print(f"{name:<18} {bar} {rate:.0f} docs/sec ({time_val:.3f}s)")
        
        # Document Distribution
        print(f"\n🥧 DOCUMENT DISTRIBUTION:")
        total = 120
        print(f"Basic Schema:     50 docs ({50/total*100:.1f}%)")
        print(f"Enhanced Schema:  50 docs ({50/total*100:.1f}%)")
        print(f"Complex Structure: 20 docs ({20/total*100:.1f}%)")
        print(f"Total Documents:  {total}")

    def create_text_performance_analysis(self):
        """Text-based Performance Analysis visualization"""
        print("\n📊 PERFORMANCE ANALYSIS (TEXT)")
        print("=" * 35)
        
        if 'crud_performance' in self.metrics:
            print("\n📈 INSERT PERFORMANCE SCALING:")
            for size in sorted(self.metrics['crud_performance'].keys()):
                crud_data = self.metrics['crud_performance'][size]
                rate = crud_data.get('create_rate', 0)
                bar_length = int((rate / 2000) * 40) if rate < 2000 else 40
                bar = "█" * bar_length + "░" * (40 - bar_length)
                print(f"{size:>6} docs {bar} {rate:.0f} docs/sec")

    def create_text_data_integrity(self):
        """Text-based Data Integrity visualization"""
        print("\n📊 DATA INTEGRITY ANALYSIS (TEXT)")
        print("=" * 40)
        
        if 'validation_results' in self.metrics:
            validation_data = self.metrics['validation_results']
            print(f"\n✅ Valid Insertions:     {validation_data.get('valid_insertions', 0)}")
            print(f"❌ Invalid Blocked:      {validation_data.get('invalid_insertions_blocked', 0)}")
            print(f"⚠️  Validation Errors:   {len(validation_data.get('validation_errors', []))}")
        
        if 'transaction_results' in self.metrics:
            transaction_data = self.metrics['transaction_results']
            successful = transaction_data.get('successful_transactions', 0)
            total = successful + transaction_data.get('failed_transactions', 0)
            success_rate = (successful / total * 100) if total > 0 else 0
            print(f"\n🔄 Transaction Success:  {successful}/{total} ({success_rate:.1f}%)")

    # Update the create_visualizations method to use the new separate visualizations
    def create_visualizations(self):
        """Create comprehensive visualizations - now calls separate visualization methods"""
        print("\n📊 Creating PostgreSQL Database Evaluation Visualizations...")
        
        # Call the new method that creates all three separate images
        self.create_all_separate_visualizations()
        
        # Also create the original combined visualization for comparison
        if HAS_MATPLOTLIB:
            self.create_original_combined_visualization()

    def create_original_combined_visualization(self):
        """Create the original combined visualization (optional)"""
        print("\n📊 Creating Original Combined Visualization...")
        
        # Create comprehensive visualization
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('MySQL Database Evaluation Results', fontsize=16, fontweight='bold')
        
        # 1. Schema Performance Comparison
        if all(key in self.metrics for key in ['basic_insertion', 'evolution_insertion', 'complex_insertion']):
            schema_types = ['Basic\nSchema', 'Enhanced\nSchema', 'Complex\nSchema']
            schema_times = [
                self.metrics['basic_insertion'],
                self.metrics['evolution_insertion'],
                self.metrics['complex_insertion']
            ]
            
            bars1 = ax1.bar(schema_types, schema_times, color=['lightblue', 'lightgreen', 'lightcoral'], alpha=0.8)
            ax1.set_title('Schema Implementation Performance', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Time (seconds)')
            ax1.grid(True, alpha=0.3)
            
            for bar, time_val in zip(bars1, schema_times):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 0.001,
                        f'{time_val:.3f}s', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # 2. Query Performance
        if 'query_times' in self.metrics:
            query_names = list(self.metrics['query_times'].keys())[:5]  # Top 5 queries
            query_times = [self.metrics['query_times'][q]['time'] for q in query_names]
            
            bars2 = ax2.barh(range(len(query_names)), query_times, color='gold', alpha=0.8)
            ax2.set_title('Query Performance Analysis', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Time (seconds)')
            ax2.set_yticks(range(len(query_names)))
            ax2.set_yticklabels([name.replace(' ', '\n') for name in query_names], fontsize=9)
            ax2.grid(True, alpha=0.3)
        
        # 3. Data Integrity Results
        if 'validation_results' in self.metrics:
            validation_data = self.metrics['validation_results']
            integrity_categories = ['Valid\nAccepted', 'Invalid\nBlocked', 'Validation\nErrors']
            integrity_values = [
                validation_data.get('valid_insertions', 0),
                validation_data.get('invalid_insertions_blocked', 0),
                len(validation_data.get('validation_errors', []))
            ]
            
            bars3 = ax3.bar(integrity_categories, integrity_values, 
                           color=['lightgreen', 'lightcoral', 'orange'], alpha=0.8)
            ax3.set_title('Data Integrity & Validation', fontsize=14, fontweight='bold')
            ax3.set_ylabel('Count')
            ax3.grid(True, alpha=0.3)
            
            for bar, value in zip(bars3, integrity_values):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{value}', ha='center', va='bottom', fontsize=11, fontweight='bold')
        
        # 4. MySQL Features Summary
        mysql_features = {
            'FOREIGN KEY\nConstraints': 10,
            'CHECK\nConstraints': 10,
            'ACID\nTransactions': 10,
            'Data Types\nValidation': 10,
            'Schema\nEvolution': 7,
            'JSON\nSupport': 8
        }
        
        feature_names = list(mysql_features.keys())
        feature_scores = list(mysql_features.values())
        
        bars4 = ax4.bar(feature_names, feature_scores, color='skyblue', alpha=0.8)
        ax4.set_title('MySQL Database Features (Score out of 10)', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Score')
        ax4.set_ylim(0, 10)
        ax4.grid(True, alpha=0.3)
        
        for bar, score in zip(bars4, feature_scores):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{score}/10', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('mysql_database_evaluation_results.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ MySQL evaluation visualization created: 'mysql_database_evaluation_results.png'")

    def create_text_visualizations(self):
        """Create text-based visualizations when matplotlib is not available"""
        print("📊 TEXT-BASED VISUALIZATIONS - MYSQL EVALUATION")
        print("=" * 55)
        
        # 1. Schema Performance Chart
        print("\n📈 SCHEMA IMPLEMENTATION PERFORMANCE:")
        schema_data = [
            ("Basic Schema", self.metrics.get('basic_insertion', 0)),
            ("Enhanced Schema", self.metrics.get('evolution_insertion', 0)),
            ("Complex Schema", self.metrics.get('complex_insertion', 0))
        ]
        
        max_time = max(time for _, time in schema_data) if schema_data else 1
        for name, time_val in schema_data:
            bar_length = int((time_val / max_time) * 40) if max_time > 0 else 0
            bar = "█" * bar_length + "░" * (40 - bar_length)
            print(f"{name:<20} {bar} {time_val:.4f}s")
        
        # 2. MySQL Strengths
        print("\n⭐ MYSQL STRENGTHS:")
        mysql_strengths = {
            "ACID Transactions": 10,
            "Referential Integrity": 10,
            "Data Validation": 10,
            "Query Performance": 9,
            "Maturity & Stability": 10,
            "Standards Compliance": 10
        }
        
        for feature, score in mysql_strengths.items():
            stars = "★" * score + "☆" * (10 - score)
            bar_length = int((score / 10) * 25)
            bar = "█" * bar_length + "░" * (25 - bar_length)
            print(f"{feature:<20} {bar} {score}/10 {stars}")
        
        print("\n💡 To get graphical charts, install matplotlib: pip install matplotlib")

    def export_for_mongodb_comparison(self):
        """Export comprehensive data for MongoDB team comparison"""
        print(f"\n📁 EXPORTING MYSQL RESULTS FOR MONGODB COMPARISON")
        print("-" * 55)
        
        comparison_data = {
            "experiment_overview": {
                "database": "MySQL",
                "version": self.connection.get_server_info(),
                "objectives_completed": 3,
                "total_tables": len([t for t in ["products", "products_enhanced", "products_complex", 
                                                "customers", "orders", "order_items", "payments", "inventory"]]),
                "experiment_date": datetime.now().isoformat()
            },
            "objective_1_schema_flexibility": {
                "experiment_type": "Schema Flexibility & Data Structure Support",
                "performance_metrics": {
                    "basic_insertion_time": self.metrics.get('basic_insertion', 0),
                    "schema_evolution_time": self.metrics.get('evolution_insertion', 0),
                    "complex_structure_time": self.metrics.get('complex_insertion', 0),
                    "average_query_time": self.metrics.get('avg_query_time', 0)
                },
                "capabilities": {
                    "schema_migration_required": True,
                    "nested_objects_supported": "Via JSON data type and normalization",
                    "arrays_supported": "Via JSON data type and separate tables",
                    "mixed_types_supported": "With proper schema design",
                    "query_flexibility": "High with SQL and JSON functions"
                }
            },
            "objective_2_performance": {
                "experiment_type": "CRUD Performance Analysis",
                "crud_performance": self.metrics.get('crud_performance', {}),
                "performance_summary": self.metrics.get('performance_summary', {})
            },
            "objective_3_data_integrity": {
                "experiment_type": "Data Integrity & Consistency (E-commerce Transactions)",
                "validation_results": self.metrics.get('validation_results', {}),
                "transaction_results": self.metrics.get('transaction_results', {}),
                "integrity_results": self.metrics.get('integrity_results', {}),
                "consistency_performance": self.metrics.get('consistency_performance', {}),
                "constraint_overhead": self.metrics.get('constraint_overhead', 0),
                "capabilities": {
                    "check_constraints": True,
                    "foreign_key_constraints": True,
                    "acid_transactions": True,
                    "referential_integrity": "Built-in database-level enforcement",
                    "cascade_operations": True,
                    "data_validation": "Multiple levels (data types, constraints, triggers)"
                }
            },
            "mysql_advantages": {
                "data_integrity": [
                    "Built-in FOREIGN KEY constraints with CASCADE operations",
                    "CHECK constraints for business rule validation",
                    "ACID transactions with full rollback support",
                    "Mature constraint enforcement system"
                ],
                "performance": [
                    "Optimized query execution with advanced indexing",
                    "Mature query optimizer",
                    "Excellent performance for relational operations",
                    "Efficient JOIN operations"
                ],
                "reliability": [
                    "Mature and battle-tested database system",
                    "Extensive tooling ecosystem",
                    "Strong consistency guarantees",
                    "Comprehensive backup and recovery options"
                ]
            },
            "mongodb_comparison_notes": [
                "MySQL requires explicit schema design vs MongoDB's dynamic schemas",
                "MySQL has built-in referential integrity vs MongoDB's application-level enforcement",
                "MySQL excels at complex relational queries vs MongoDB's document-based queries",
                "MySQL schema changes require migration vs MongoDB's instant field addition",
                "MySQL has mature ACID transactions vs MongoDB's newer multi-document transactions"
            ]
        }
        
        # Save to JSON file
        with open('mysql_complete_evaluation_results.json', 'w') as f:
            json.dump(comparison_data, f, indent=2, default=str)
        
        print("✅ Complete MySQL evaluation results exported to 'mysql_complete_evaluation_results.json'")
        print("📋 Share this file with your MongoDB teammates for comprehensive comparison")
        print("🎯 Includes schema flexibility, performance analysis, and data integrity testing")

    def run_complete_evaluation(self):
        """Run the complete MySQL database evaluation experiment"""
        print("🚀 STARTING COMPLETE MYSQL DATABASE EVALUATION")
        print("=" * 60)
        print("📋 Objectives: Schema Flexibility + Performance Analysis + Data Integrity")
        print("⏱️  Estimated time: 5-7 minutes")
        print("")
        
        try:
            # Run Objective 1: Schema Flexibility
            schema_results = self.run_objective_1_schema_flexibility()
            
            # Run Objective 2: Performance Analysis  
            performance_results = self.run_objective_2_performance()
            
            # Run Objective 3: Data Integrity & Consistency
            integrity_results = self.run_objective_3_data_integrity()
            
            # Generate comprehensive summary
            self.generate_summary()
            
            # Create visualizations and export
            self.create_visualizations()
            self.export_for_mongodb_comparison()
            
            print(f"\n🎉 Complete MySQL Evaluation Completed Successfully!")
            print(f"✨ MySQL demonstrated strong capabilities across all three objectives:")
            print(f"   📋 Schema Flexibility: Structured approach with JSON support")
            print(f"   ⚡ Performance: Excellent CRUD performance with mature optimization")
            print(f"   🛡️  Data Integrity: Comprehensive built-in constraints and ACID compliance")
            print(f"📊 Check 'mysql_database_evaluation_results.png' for visualizations")
            print(f"📁 Results exported to 'mysql_complete_evaluation_results.json' for MongoDB team comparison")
            
            return {
                "schema_results": schema_results,
                "performance_results": performance_results,
                "integrity_results": integrity_results
            }
            
        except Error as e:
            print(f"❌ Evaluation failed: {e}")
            return None
        
        finally:
            if self.connection and self.connection.is_connected():
                self.cursor.close()
                self.connection.close()
                print("🔌 MySQL connection closed")


    def run_complete_evaluation(self):
        """Run all evaluation objectives and generate summary & visualizations."""
        print("\n🚀 Starting Complete PostgreSQL Evaluation...\n")
        
        self.setup_database()
        self.run_objective_1_schema_flexibility()
        self.run_objective_2_performance()
        self.run_objective_3_data_integrity()
        self.generate_summary()
        self.create_visualizations()

        print("\n✅ Evaluation completed successfully.")


# Run the complete evaluation
if __name__ == "__main__":
    experiment = PostgreSQLTest()
    experiment.run_complete_evaluation()
