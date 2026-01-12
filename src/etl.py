import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data(file_path):
    """
    EXTRACT: Read data from CSV file
    In real companies: This could be API calls, database queries, streaming data
    """
    logger.info(f"Extracting data from {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def transform_data(df):
    """
    TRANSFORM: Clean and prepare data
    Why transformations matter: Raw data is messy, we need it structured for analysis
    """
    logger.info("Transforming data...")
    
    # Create a copy to avoid modifying original
    df_transformed = df.copy()
    
    # 1. Fix data types
    df_transformed['date'] = pd.to_datetime(df_transformed['date'])
    df_transformed['price'] = pd.to_numeric(df_transformed['price'])
    df_transformed['units_sold'] = pd.to_numeric(df_transformed['units_sold'])
    df_transformed['rating'] = pd.to_numeric(df_transformed['rating'])
    
    # 2. Handle missing values (if any)
    # In real data: You might fill with median, mean, or use business logic
    df_transformed = df_transformed.dropna()
    
    # 3. Create derived columns (business logic)
    df_transformed['revenue'] = df_transformed['price'] * df_transformed['units_sold']
    df_transformed['month'] = df_transformed['date'].dt.month_name()
    df_transformed['day_of_week'] = df_transformed['date'].dt.day_name()
    
    # 4. Standardize text data
    df_transformed['category'] = df_transformed['category'].str.title()
    df_transformed['author'] = df_transformed['author'].str.title()
    
    logger.info(f"Transformation complete. Added revenue column, cleaned categories")
    return df_transformed

def load_data(df, table_name):
    """
    LOAD: Insert data into PostgreSQL database
    Why databases? They're optimized for storing/querying structured data
    """
    logger.info(f"Loading data to PostgreSQL table: {table_name}")
    
    # Database connection string
    # Format: postgresql://username:password@host:port/database
    engine = create_engine('postgresql://bookstore:password123@postgres:5432/bookstore_db')
    
    try:
        # Create table and insert data
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
        
        # Create additional tables for analytics (normalized structure)
        with engine.connect() as conn:
            # Create dimension tables
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_books (
                    book_id SERIAL PRIMARY KEY,
                    title VARCHAR(255),
                    author VARCHAR(255),
                    category VARCHAR(100)
                )
            """))
            
            # Insert unique books
            unique_books = df[['title', 'author', 'category']].drop_duplicates()
            for _, book in unique_books.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_books (title, author, category)
                    VALUES (:title, :author, :category)
                    ON CONFLICT (title, author) DO NOTHING
                """), {
                    'title': book['title'],
                    'author': book['author'],
                    'category': book['category']
                })
            
            logger.info("Created dimension tables for analytics")
            
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def run_analytics():
    """
    Run sample analytical queries
    Shows how data gets consumed by analysts/business users
    """
    logger.info("Running analytical queries...")
    
    engine = create_engine('postgresql://bookstore:password123@postgres:5432/bookstore_db')
    
    queries = {
        "total_revenue": """
            SELECT SUM(revenue) as total_revenue 
            FROM book_sales
        """,
        "top_selling_books": """
            SELECT title, SUM(units_sold) as total_units_sold, SUM(revenue) as total_revenue
            FROM book_sales
            GROUP BY title
            ORDER BY total_units_sold DESC
            LIMIT 5
        """,
        "category_performance": """
            SELECT category, 
                   COUNT(*) as num_books,
                   SUM(units_sold) as total_units_sold,
                   SUM(revenue) as total_revenue,
                   AVG(rating) as avg_rating
            FROM book_sales
            GROUP BY category
            ORDER BY total_revenue DESC
        """,
        "daily_sales_trend": """
            SELECT date, 
                   SUM(units_sold) as daily_units,
                   SUM(revenue) as daily_revenue
            FROM book_sales
            GROUP BY date
            ORDER BY date
        """
    }
    
    results = {}
    with engine.connect() as conn:
        for query_name, query in queries.items():
            result = conn.execute(text(query))
            results[query_name] = result.fetchall()
            logger.info(f"\n--- {query_name.upper().replace('_', ' ')} ---")
            for row in results[query_name]:
                logger.info(row)
    
    return results

def main():
    """Main ETL pipeline"""
    logger.info("Starting ETL Pipeline")
    
    # ETL Process
    raw_data = extract_data('/data/raw_sales.csv')
    transformed_data = transform_data(raw_data)
    load_data(transformed_data, 'book_sales')
    
    # Analytics
    results = run_analytics()
    
    logger.info("ETL Pipeline completed successfully!")
    return results

if __name__ == "__main__":
    main()