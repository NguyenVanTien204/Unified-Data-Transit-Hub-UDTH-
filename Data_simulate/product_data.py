from faker import Faker
from pymongo import MongoClient
from datetime import datetime, timedelta
import uuid
import random
import json
import os

# ----- CONFIG -----
fake = Faker()
client = MongoClient("mongodb://localhost:27017/")
db = client["mock_shop"]

NUM_MESSAGES = 200
NUM_RATINGS = 300
WAREHOUSES = ["north", "south", "central", "east", "west"]

# Load shared data from MySQL generation
def load_shared_data():
    if os.path.exists('shared_data.json'):
        with open('shared_data.json', 'r') as f:
            return json.load(f)
    else:
        print("Warning: shared_data.json not found. Using mock data.")
        return {
            'user_ids': [str(uuid.uuid4()) for _ in range(50)],
            'product_ids': [str(uuid.uuid4()) for _ in range(50)]
        }

SHARED_DATA = load_shared_data()
USER_IDS = SHARED_DATA['user_ids']
PRODUCT_IDS = SHARED_DATA['product_ids']

# ----- ENHANCED GENERATORS -----

def generate_enhanced_products():
    """Generate enhanced product data synchronized with MySQL"""
    products = []
    categories = ['Electronics', 'Fashion', 'Books', 'Home & Garden', 'Sports', 'Beauty', 'Toys', 'Automotive']
    brands = ['Apple', 'Samsung', 'Nike', 'Adidas', 'IKEA', 'Sony', 'Generic', 'NoName']
    
    # Tag pool for realistic product tagging
    tag_pools = {
        'Electronics': ['bluetooth', 'wireless', 'waterproof', 'fast-charging', 'hd', '4k'],
        'Fashion': ['cotton', 'summer', 'winter', 'casual', 'formal', 'trendy'],
        'Books': ['bestseller', 'fiction', 'non-fiction', 'educational', 'paperback', 'hardcover'],
        'Home & Garden': ['eco-friendly', 'modern', 'vintage', 'durable', 'compact'],
        'Sports': ['professional', 'beginner', 'outdoor', 'indoor', 'fitness'],
        'Beauty': ['organic', 'anti-aging', 'moisturizing', 'for-sensitive-skin'],
        'Toys': ['educational', 'ages-3+', 'safe', 'interactive', 'creative'],
        'Automotive': ['premium', 'universal', 'easy-install', 'durable']
    }
    
    for pid in PRODUCT_IDS:
        category = random.choice(categories)
        brand = random.choice(brands)
        
        # Generate realistic product name
        adjectives = ['Premium', 'Ultra', 'Pro', 'Classic', 'Modern', 'Smart', 'Advanced']
        nouns = ['Device', 'Tool', 'Equipment', 'Accessory', 'Kit', 'Set', 'System']
        
        name_parts = []
        if random.random() > 0.3:
            name_parts.append(random.choice(adjectives))
        name_parts.append(fake.word().capitalize())
        if random.random() > 0.5:
            name_parts.append(random.choice(nouns))
        
        name = ' '.join(name_parts)
        
        # 3% chance of duplicate names (realistic scenario)
        if random.random() < 0.03:
            name = random.choice(["Generic Phone", "Magic Shirt", "Wooden Table", "Universal Charger"])
        
        # Price based on category and brand
        base_price = {
            'Electronics': random.uniform(50, 2000),
            'Fashion': random.uniform(20, 500),
            'Books': random.uniform(10, 50),
            'Home & Garden': random.uniform(25, 800),
            'Sports': random.uniform(15, 300),
            'Beauty': random.uniform(10, 200),
            'Toys': random.uniform(10, 100),
            'Automotive': random.uniform(20, 500)
        }
        
        price = base_price.get(category, random.uniform(15, 300))
        
        # Brand premium
        if brand in ['Apple', 'Samsung', 'Nike', 'Adidas']:
            price *= random.uniform(1.2, 2.0)
        
        # 2% chance of pricing errors
        if random.random() < 0.02:
            price = random.uniform(-20, 0)  # Negative price
        
        price = round(price, 2)
        
        # Generate realistic specs
        specs = {
            "brand": brand,
            "warranty": f"{random.randint(6, 36)} months",
            "weight": f"{round(random.uniform(0.1, 10.0), 2)} kg",
            "origin": random.choice(["China", "USA", "Germany", "Japan", "Vietnam", "Unknown"]),
        }
        
        # Category-specific specs
        if category == 'Electronics':
            specs.update({
                "power": f"{random.randint(5, 200)}W",
                "connectivity": random.choice(["USB", "Bluetooth", "WiFi", "USB-C"]),
                "display": f"{random.uniform(4, 65):.1f} inch" if random.random() > 0.5 else None
            })
        elif category == 'Fashion':
            specs.update({
                "material": random.choice(["Cotton", "Polyester", "Silk", "Wool", "Synthetic"]),
                "care": random.choice(["Machine wash", "Hand wash", "Dry clean only"])
            })
        elif category == 'Books':
            specs.update({
                "pages": random.randint(100, 800),
                "language": random.choice(["English", "Vietnamese", "Spanish", "French"]),
                "format": random.choice(["Paperback", "Hardcover", "Ebook"])
            })
        
        # 3% chance of missing critical specs
        if random.random() < 0.03:
            specs["material"] = None
        
        # Generate variants with realistic logic
        variants = []
        num_variants = random.randint(1, 5)
        
        colors = ["red", "blue", "green", "black", "white", "yellow", "purple", "orange"]
        sizes = ["XS", "S", "M", "L", "XL", "XXL"] if category == 'Fashion' else ["Small", "Medium", "Large"]
        
        for _ in range(num_variants):
            variant = {
                "sku": str(uuid.uuid4()),
                "color": random.choice(colors),
                "size": random.choice(sizes) if category in ['Fashion', 'Sports'] else None,
                "price_diff": round(random.uniform(-10, 50), 2),
                "stock": random.randint(0, 100)
            }
            
            # 2% chance of invalid variant data
            if random.random() < 0.02:
                variant["stock"] = -1  # Invalid stock
            
            # 1% chance of nested variant data (data quality issue)
            if random.random() < 0.01:
                variant["nested_data"] = {
                    "weight": round(random.uniform(0.1, 5.0), 2),
                    "dimensions": f"{random.randint(10, 50)}x{random.randint(10, 50)}x{random.randint(5, 20)}cm"
                }
            
            variants.append(variant)
        
        # Generate tags
        category_tags = tag_pools.get(category, ['general', 'product'])
        tags = random.sample(category_tags, k=random.randint(1, 3))
        
        # 2% chance of malformed tags (string instead of array)
        if random.random() < 0.02:
            tags = ", ".join(tags)
        
        # Generate realistic timestamps
        created_at = fake.date_time_between(start_date="-2y", end_date="-1M")
        updated_at = fake.date_time_between(start_date=created_at, end_date="now")
        
        product = {
            "_id": pid,
            "name": name,
            "category": category,
            "brand": brand,
            "price": price,
            "description": fake.text(max_nb_chars=500),
            "available": random.choice([True, False]),
            "tags": tags,
            "specs": specs,
            "variants": variants,
            "images": [
                f"https://cdn.example.com/products/{pid}/image_{i}.jpg" 
                for i in range(random.randint(1, 5))
            ],
            "seo": {
                "title": f"{name} - {brand}",
                "description": fake.sentence(nb_words=15),
                "keywords": tags if isinstance(tags, list) else [tags]
            },
            "inventory": {
                "total_stock": sum(v.get("stock", 0) for v in variants),
                "reserved": random.randint(0, 10),
                "safety_stock": random.randint(5, 20)
            },
            "created_at": created_at,
            "updated_at": updated_at,
            "status": random.choice(["active", "inactive", "discontinued"]),
            "featured": random.random() < 0.1,  # 10% featured products
            "sales_count": random.randint(0, 1000),
            "view_count": random.randint(0, 10000)
        }
        
        products.append(product)
    
    return products

def generate_enhanced_stock_levels():
    """Generate realistic stock levels with warehouse distribution"""
    stock_levels = []
    
    for pid in PRODUCT_IDS:
        # Generate realistic stock distribution
        total_stock = random.randint(0, 1000)
        
        # 5% chance of stock inconsistency
        if random.random() < 0.05:
            total_stock = random.randint(-50, 0)  # Negative or zero stock
        
        warehouses_data = {}
        remaining_stock = total_stock
        
        for i, warehouse in enumerate(WAREHOUSES):
            if i == len(WAREHOUSES) - 1:
                # Last warehouse gets remaining stock
                stock = remaining_stock
            else:
                # Distribute stock randomly
                stock = random.randint(0, max(1, remaining_stock // 2))
                remaining_stock -= stock
            
            # 3% chance of warehouse data issues
            if random.random() < 0.03:
                stock = random.randint(-10, 0)  # Negative stock
            
            warehouses_data[warehouse] = {
                "stock": stock,
                "reserved": random.randint(0, max(1, stock // 10)),
                "last_updated": fake.date_time_between(start_date="-30d", end_date="now"),
                "reorder_point": random.randint(10, 50),
                "max_capacity": random.randint(200, 2000),
                "location": f"Zone-{random.choice(['A', 'B', 'C'])}-{random.randint(1, 100)}",
                "temperature_controlled": random.choice([True, False]),
                "manager": fake.name()
            }
        
        stock_level = {
            "product_id": pid,
            "total_available": total_stock,
            "warehouses": warehouses_data,
            "last_inventory_check": fake.date_time_between(start_date="-7d", end_date="now"),
            "pending_orders": random.randint(0, 50),
            "incoming_shipments": [
                {
                    "shipment_id": str(uuid.uuid4()),
                    "quantity": random.randint(50, 200),
                    "expected_date": fake.date_time_between(start_date="now", end_date="+30d"),
                    "supplier": fake.company(),
                    "status": random.choice(["pending", "in_transit", "delayed"])
                }
                for _ in range(random.randint(0, 3))
            ],
            "alerts": [
                {
                    "type": random.choice(["low_stock", "overstock", "expired", "damaged"]),
                    "warehouse": random.choice(WAREHOUSES),
                    "quantity": random.randint(1, 20),
                    "created_at": fake.date_time_between(start_date="-7d", end_date="now"),
                    "resolved": random.choice([True, False])
                }
                for _ in range(random.randint(0, 2))
            ] if random.random() < 0.3 else []
        }
        
        stock_levels.append(stock_level)
    
    return stock_levels


def generate_enhanced_user_messages():
    """Generate realistic customer support conversations"""
    conversations = []
    
    # Message templates for different scenarios
    issue_templates = {
        "order_inquiry": [
            "Hi, I'd like to check the status of my order {}",
            "Where is my order? It's been {} days",
            "Can you update me on order {}?"
        ],
        "product_question": [
            "Does this product work with {}?",
            "What's the warranty on this item?",
            "Is this available in different colors?"
        ],
        "complaint": [
            "I received a damaged product",
            "The item doesn't match the description",
            "Very disappointed with the quality"
        ],
        "return_request": [
            "I need to return this item",
            "How do I process a return?",
            "The product is not what I expected"
        ],
        "billing_issue": [
            "I was charged twice for the same order",
            "Can you explain this charge?",
            "I need a refund for order {}"
        ]
    }
    
    for _ in range(NUM_MESSAGES):
        user_id = random.choice(USER_IDS)
        thread_id = str(uuid.uuid4())
        
        # Choose conversation type
        conversation_type = random.choice(list(issue_templates.keys()))
        
        # Generate conversation flow
        messages = []
        base_time = fake.date_time_between(start_date="-3M", end_date="now")
        
        # Initial customer message
        template = random.choice(issue_templates[conversation_type])
        if "{}" in template:
            if "order" in template:
                content = template.format(f"ORD-{random.randint(100000, 999999)}")
            elif "days" in template:
                content = template.format(random.randint(3, 14))
            else:
                content = template.format(fake.word())
        else:
            content = template
        
        # 3% chance of empty/null content (data quality issue)
        if random.random() < 0.03:
            content = None
        
        messages.append({
            "message_id": str(uuid.uuid4()),
            "sender": user_id,
            "sender_type": "customer",
            "receiver": "support",
            "timestamp": base_time,
            "content": content,
            "message_type": "text",
            "priority": random.choice(["low", "medium", "high"]),
            "channel": random.choice(["web", "email", "phone", "chat"]),
            "attachments": [
                {
                    "filename": f"receipt_{random.randint(1000, 9999)}.pdf",
                    "url": f"https://storage.example.com/attachments/{uuid.uuid4()}.pdf",
                    "size": random.randint(50000, 500000)
                }
            ] if random.random() < 0.15 else []
        })
        
        # Generate support responses and follow-ups
        num_exchanges = random.randint(1, 6)
        current_time = base_time
        
        for i in range(num_exchanges):
            # Support response
            response_delay = random.randint(5, 180)  # 5 minutes to 3 hours
            current_time += timedelta(minutes=response_delay)
            
            support_responses = [
                "Thank you for contacting us. Let me look into this.",
                "I understand your concern. Let me check your account.",
                "I apologize for the inconvenience. Here's what I found:",
                "I've escalated this to our team. You'll hear back within 24 hours.",
                "Is there anything else I can help you with today?"
            ]
            
            messages.append({
                "message_id": str(uuid.uuid4()),
                "sender": f"agent_{random.randint(1, 20)}",
                "sender_type": "support",
                "receiver": user_id,
                "timestamp": current_time,
                "content": random.choice(support_responses),
                "message_type": "text",
                "priority": "medium",
                "channel": "web",
                "internal_notes": fake.sentence() if random.random() < 0.3 else None
            })
            
            # Customer follow-up (70% chance)
            if random.random() < 0.7 and i < num_exchanges - 1:
                followup_delay = random.randint(10, 300)
                current_time += timedelta(minutes=followup_delay)
                
                followup_responses = [
                    "Thank you for the quick response!",
                    "That helps, but I have another question...",
                    "I'm still not satisfied with this solution.",
                    "Perfect, that resolved my issue!",
                    "When can I expect an update?"
                ]
                
                messages.append({
                    "message_id": str(uuid.uuid4()),
                    "sender": user_id,
                    "sender_type": "customer",
                    "receiver": "support",
                    "timestamp": current_time,
                    "content": random.choice(followup_responses),
                    "message_type": "text",
                    "priority": "medium",
                    "channel": "web"
                })
        
        # 2% chance of message ordering issues (timestamps out of order)
        if random.random() < 0.02:
            random.shuffle(messages)
        
        conversation = {
            "thread_id": thread_id,
            "user_id": user_id,
            "conversation_type": conversation_type,
            "status": random.choice(["open", "closed", "escalated", "pending"]),
            "priority": random.choice(["low", "medium", "high"]),
            "assigned_agent": f"agent_{random.randint(1, 20)}",
            "created_at": base_time,
            "updated_at": current_time,
            "satisfaction_rating": random.randint(1, 5) if random.random() < 0.6 else None,
            "resolved": random.choice([True, False]),
            "resolution_time_minutes": int((current_time - base_time).total_seconds() / 60),
            "messages": messages,
            "tags": random.sample(["billing", "shipping", "product", "return", "complaint"], k=random.randint(1, 3))
        }
        
        conversations.append(conversation)
    
    return conversations


def generate_enhanced_ratings():
    """Generate realistic product ratings with detailed feedback"""
    ratings = []
    
    # Rating distribution (more realistic)
    rating_weights = [5, 10, 15, 35, 35]  # 1-5 stars
    
    # Sentiment words for different ratings
    sentiment_words = {
        1: ["terrible", "awful", "disappointed", "waste", "broken", "useless"],
        2: ["poor", "mediocre", "below average", "issues", "problems"],
        3: ["okay", "average", "decent", "acceptable", "mixed feelings"],
        4: ["good", "satisfied", "quality", "recommended", "solid"],
        5: ["excellent", "amazing", "perfect", "outstanding", "love it"]
    }
    
    for _ in range(NUM_RATINGS):
        product_id = random.choice(PRODUCT_IDS)
        user_id = random.choice(USER_IDS)
        
        # Generate rating with realistic distribution
        rating = random.choices(range(1, 6), weights=rating_weights)[0]
        
        # 3% chance of invalid rating (data quality issue)
        if random.random() < 0.03:
            rating = random.choice([0, 6, 7, -1])
        
        # Verified purchase (80% of ratings)
        verified_purchase = random.random() < 0.8
        
        # Generate tags based on rating
        if rating <= 2:
            tag_pool = ["damaged", "poor quality", "not as described", "slow delivery", "expensive"]
        elif rating == 3:
            tag_pool = ["average quality", "okay price", "mixed experience", "decent"]
        else:
            tag_pool = ["fast delivery", "good quality", "excellent value", "recommended", "satisfied"]
        
        # Select tags
        tags = random.sample(tag_pool, k=random.randint(1, 3))
        
        # 2% chance of malformed tags (string instead of array)
        if random.random() < 0.02:
            tags = ", ".join(tags)
        
        # Generate comment based on rating
        base_words = sentiment_words.get(rating, ["neutral"])
        comment_templates = [
            "This product is {}. {}",
            "I found this to be {}. {}",
            "{} purchase. {}",
            "The quality is {}. {}"
        ]
        
        sentiment = random.choice(base_words)
        additional_comment = fake.sentence(nb_words=random.randint(10, 30))
        comment = random.choice(comment_templates).format(sentiment, additional_comment)
        
        # 5% chance of very long comments (edge case)
        if random.random() < 0.05:
            comment = fake.text(max_nb_chars=2000)
        
        # 2% chance of empty comment
        if random.random() < 0.02:
            comment = None
        
        # Generate helpful votes
        helpful_votes = random.randint(0, 100) if rating in [1, 2, 4, 5] else random.randint(0, 20)
        
        # Rating timestamp
        rating_date = fake.date_time_between(start_date="-1y", end_date="now")
        
        # Additional metadata
        rating_data = {
            "rating_id": str(uuid.uuid4()),
            "product_id": product_id,
            "user_id": user_id,
            "rating": rating,
            "verified_purchase": verified_purchase,
            "tags": tags,
            "comment": comment,
            "helpful_votes": helpful_votes,
            "total_votes": helpful_votes + random.randint(0, 20),
            "timestamp": rating_date,
            "updated_at": rating_date,
            "status": random.choice(["active", "flagged", "removed"]),
            "language": random.choice(["en", "vi", "es", "fr"]),
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "location": {
                "country": fake.country(),
                "city": fake.city()
            },
            "images": [
                f"https://cdn.example.com/reviews/{uuid.uuid4()}.jpg"
                for _ in range(random.randint(0, 3))
            ] if random.random() < 0.2 else [],
            "response": {
                "from": "seller",
                "content": fake.sentence(nb_words=20),
                "timestamp": rating_date + timedelta(days=random.randint(1, 7))
            } if random.random() < 0.15 else None
        }
        
        ratings.append(rating_data)
    
    return ratings


def generate_analytics_data():
    """Generate additional analytics/tracking data"""
    analytics = []
    
    for _ in range(500):  # Generate 500 analytics events
        event_types = ["product_view", "add_to_cart", "remove_from_cart", "purchase", "search", "filter"]
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": random.choice(event_types),
            "user_id": random.choice(USER_IDS) if random.random() > 0.3 else None,  # 30% anonymous
            "product_id": random.choice(PRODUCT_IDS) if random.random() > 0.2 else None,
            "timestamp": fake.date_time_between(start_date="-30d", end_date="now"),
            "session_id": str(uuid.uuid4()),
            "user_agent": fake.user_agent(),
            "ip_address": fake.ipv4(),
            "referrer": random.choice([
                "https://google.com",
                "https://facebook.com",
                "direct",
                "https://instagram.com",
                None
            ]),
            "page_url": f"https://shop.example.com/product/{random.choice(PRODUCT_IDS)}",
            "metadata": {
                "search_query": fake.word() if random.random() < 0.3 else None,
                "filter_applied": random.choice(["price", "category", "brand", "rating"]) if random.random() < 0.2 else None,
                "cart_total": round(random.uniform(10, 500), 2) if random.random() < 0.4 else None
            }
        }
        
        analytics.append(event)
    
    return analytics


# ----- MAIN FUNCTION -----
def insert_enhanced_data():
    """Insert all enhanced data into MongoDB"""
    
    print("Generating enhanced products...")
    products = generate_enhanced_products()
    
    print("Generating enhanced stock levels...")
    stock_levels = generate_enhanced_stock_levels()
    
    print("Generating enhanced user messages...")
    messages = generate_enhanced_user_messages()
    
    print("Generating enhanced ratings...")
    ratings = generate_enhanced_ratings()
    
    print("Generating analytics data...")
    analytics = generate_analytics_data()
    
    # Drop existing collections
    collections = ['products', 'stock_levels', 'user_messages', 'ratings', 'analytics']
    for collection in collections:
        db[collection].drop()
        print(f"Dropped collection: {collection}")
    
    # Insert new data
    db.products.insert_many(products)
    db.stock_levels.insert_many(stock_levels)
    db.user_messages.insert_many(messages)
    db.ratings.insert_many(ratings)
    db.analytics.insert_many(analytics)
    
    # Create indexes for better performance
    db.products.create_index("category")
    db.products.create_index("brand")
    db.products.create_index("price")
    db.products.create_index("created_at")
    
    db.stock_levels.create_index("product_id")
    db.user_messages.create_index("user_id")
    db.user_messages.create_index("created_at")
    db.ratings.create_index("product_id")
    db.ratings.create_index("user_id")
    db.ratings.create_index("rating")
    db.analytics.create_index("event_type")
    db.analytics.create_index("timestamp")
    
    print(f"[✓] Inserted {len(products)} products")
    print(f"[✓] Inserted {len(stock_levels)} stock levels")
    print(f"[✓] Inserted {len(messages)} user messages")
    print(f"[✓] Inserted {len(ratings)} ratings")
    print(f"[✓] Inserted {len(analytics)} analytics events")
    print("[✓] Created database indexes")
    
    # Generate data quality report
    generate_data_quality_report(products, stock_levels, messages, ratings)


def generate_data_quality_report(products, stock_levels, messages, ratings):
    """Generate a report of intentional data quality issues"""
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": {
            "products": len(products),
            "stock_levels": len(stock_levels),
            "messages": len(messages),
            "ratings": len(ratings)
        },
        "data_quality_issues": {
            "products": {
                "negative_prices": sum(1 for p in products if p.get("price", 0) < 0),
                "missing_categories": sum(1 for p in products if not p.get("category")),
                "duplicate_names": len(products) - len(set(p["name"] for p in products)),
                "malformed_tags": sum(1 for p in products if isinstance(p.get("tags"), str)),
                "invalid_variants": sum(1 for p in products for v in p.get("variants", []) if v.get("stock", 0) < 0)
            },
            "stock_levels": {
                "negative_stock": sum(1 for s in stock_levels if s.get("total_available", 0) < 0),
                "warehouse_inconsistencies": sum(1 for s in stock_levels 
                                               for w in s.get("warehouses", {}).values() 
                                               if w.get("stock", 0) < 0)
            },
            "messages": {
                "null_content": sum(1 for m in messages 
                                  for msg in m.get("messages", []) 
                                  if msg.get("content") is None),
                "timestamp_issues": sum(1 for m in messages 
                                      if len(m.get("messages", [])) > 1 and 
                                      m["messages"][0]["timestamp"] > m["messages"][-1]["timestamp"])
            },
            "ratings": {
                "invalid_ratings": sum(1 for r in ratings if r.get("rating", 0) not in [1, 2, 3, 4, 5]),
                "malformed_tags": sum(1 for r in ratings if isinstance(r.get("tags"), str)),
                "null_comments": sum(1 for r in ratings if r.get("comment") is None)
            }
        }
    }
    
    with open("data_quality_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print("\n[✓] Data quality report generated: data_quality_report.json")
    print("\nSummary of intentional data quality issues:")
    for category, issues in report["data_quality_issues"].items():
        print(f"\n{category.upper()}:")
        for issue, count in issues.items():
            if count > 0:
                print(f"  - {issue}: {count} records")


if __name__ == "__main__":
    insert_enhanced_data()