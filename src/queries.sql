-- SQL is the language of data analysis
-- Why SQL? It's declarative - you say WHAT you want, not HOW to get it

-- 1. Basic aggregation (what analysts do daily)
SELECT 
    category,
    COUNT(*) as number_of_books,
    SUM(units_sold) as total_units,
    ROUND(SUM(revenue), 2) as total_revenue,
    ROUND(AVG(rating), 2) as average_rating
FROM book_sales
GROUP BY category
ORDER BY total_revenue DESC;

-- 2. Daily sales trend (for dashboards)
SELECT 
    date,
    SUM(units_sold) as daily_sales,
    SUM(revenue) as daily_revenue,
    ROUND(AVG(price), 2) as avg_price
FROM book_sales
GROUP BY date
ORDER BY date;

-- 3. Author performance (business question)
SELECT 
    author,
    COUNT(DISTINCT title) as books_published,
    SUM(units_sold) as total_copies_sold,
    ROUND(SUM(revenue), 2) as author_revenue
FROM book_sales
GROUP BY author
ORDER BY author_revenue DESC;

-- 4. Price analysis (pricing strategy)
SELECT 
    CASE 
        WHEN price < 10 THEN 'Budget (< $10)'
        WHEN price BETWEEN 10 AND 20 THEN 'Mid-range ($10-$20)'
        ELSE 'Premium (> $20)'
    END as price_segment,
    COUNT(*) as number_of_books,
    ROUND(AVG(units_sold), 2) as avg_units_sold,
    ROUND(AVG(rating), 2) as avg_rating
FROM book_sales
GROUP BY price_segment
ORDER BY avg_units_sold DESC;

-- 5. Join with dimension table (normalized structure)
SELECT 
    b.category,
    b.author,
    s.date,
    s.units_sold,
    s.revenue
FROM book_sales s
JOIN dim_books b ON s.title = b.title AND s.author = b.author
WHERE b.category = 'Fantasy'
ORDER BY s.date;