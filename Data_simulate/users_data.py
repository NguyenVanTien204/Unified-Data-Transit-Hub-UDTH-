import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import uuid
import json
import pytz
from sqlalchemy import create_engine, Table, Column, String, Date, DateTime, MetaData, Text, Float, Integer, Boolean, Enum
import os

# ---------- CONFIG ----------
fake = Faker()
NUM_INTERACTIONS = 3000
NUM_SESSIONS = 1500
NUM_PREFERENCES = 800
NUM_NOTIFICATIONS = 1000

INTERACTION_TYPES = ['product_view', 'product_like', 'product_comment', 'product_rating', 'add_to_cart', 'purchase', 'search', 'filter_apply', 'wishlist_add', 'review_write']
DEVICE_TYPES = ['desktop', 'mobile', 'tablet', 'smart_tv', 'smartwatch']
BROWSERS = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
OS_TYPES = ['Windows', 'macOS', 'Linux', 'iOS', 'Android']

TIMEZONES = [
    "UTC", "Asia/Ho_Chi_Minh", "Asia/Tokyo", "Europe/London",
    "America/New_York", "Australia/Sydney", "Europe/Moscow",
    "Africa/Nairobi", "Asia/Kolkata", "America/Los_Angeles"
]

# PostgreSQL connection string
PG_URL = "postgresql+psycopg2://username:password@localhost:5432/mydb"  # ← Update this
engine = create_engine(PG_URL)
metadata = MetaData()

# Load shared data from MySQL generation
def load_shared_data():
    if os.path.exists('shared_data.json'):
        with open('shared_data.json', 'r') as f:
            return json.load(f)
    else:
        print("Warning: shared_data.json not found. Using mock data.")
        return {
            'user_ids': [str(uuid.uuid4()) for _ in range(100)],
            'product_ids': [str(uuid.uuid4()) for _ in range(50)]
        }

SHARED_DATA = load_shared_data()
USER_IDS = SHARED_DATA['user_ids']
PRODUCT_IDS = SHARED_DATA['product_ids']

# ---------- DEFINE ENHANCED TABLES ----------
users_table = Table(
    "users", metadata,
    Column("user_id", String, primary_key=True),
    Column("name", String),
    Column("email", String),
    Column("gender", String),
    Column("dob", Date),
    Column("country", String),
    Column("city", String),
    Column("signup_date", Date),
    Column("last_login", DateTime(timezone=True)),
    Column("is_active", Boolean),
    Column("customer_tier", String),
    Column("total_spent", Float),
    Column("preferred_language", String),
    Column("marketing_consent", Boolean),
    Column("phone", String),
    Column("timezone", String)
)

interactions_table = Table(
    "interactions", metadata,
    Column("interaction_id", String, primary_key=True),
    Column("user_id", String),
    Column("session_id", String),
    Column("type", String),
    Column("target_id", String),
    Column("timestamp", DateTime(timezone=True)),
    Column("metadata", Text),
    Column("device_type", String),
    Column("browser", String),
    Column("os", String),
    Column("ip_address", String),
    Column("user_agent", String),
    Column("referrer", String),
    Column("duration_seconds", Integer),
    Column("success", Boolean)
)

sessions_table = Table(
    "user_sessions", metadata,
    Column("session_id", String, primary_key=True),
    Column("user_id", String),
    Column("start_time", DateTime(timezone=True)),
    Column("end_time", DateTime(timezone=True)),
    Column("device_type", String),
    Column("browser", String),
    Column("os", String),
    Column("ip_address", String),
    Column("location_country", String),
    Column("location_city", String),
    Column("total_interactions", Integer),
    Column("pages_visited", Integer),
    Column("conversion_event", String),
    Column("exit_page", String),
    Column("session_duration_minutes", Integer)
)

preferences_table = Table(
    "user_preferences", metadata,
    Column("preference_id", String, primary_key=True),
    Column("user_id", String),
    Column("category", String),
    Column("preference_type", String),
    Column("preference_value", String),
    Column("confidence_score", Float),
    Column("last_updated", DateTime(timezone=True)),
    Column("source", String),
    Column("is_explicit", Boolean)
)

notifications_table = Table(
    "user_notifications", metadata,
    Column("notification_id", String, primary_key=True),
    Column("user_id", String),
    Column("type", String),
    Column("title", String),
    Column("content", Text),
    Column("sent_at", DateTime(timezone=True)),
    Column("read_at", DateTime(timezone=True)),
    Column("clicked_at", DateTime(timezone=True)),
    Column("channel", String),
    Column("priority", String),
    Column("category", String),
    Column("metadata", Text)
)

metadata.create_all(engine)

# ---------- ENHANCED GENERATORS ----------
def generate_enhanced_users():
    """Generate enhanced user profiles synchronized with MySQL data"""
    users = []
    
    # Get user profiles from MySQL (if available)
    mysql_users = {}
    if os.path.exists('shared_data.json'):
        try:
            with open('shared_data.json', 'r') as f:
                data = json.load(f)
                mysql_users = data.get('user_profiles', {})
        except:
            pass
    
    for uid in USER_IDS:
        # Use MySQL user data if available
        mysql_profile = mysql_users.get(uid, {})
        
        # Generate consistent user data
        fake.seed_instance(hash(uid) % 2**32)  # Consistent fake data per user
        
        # Basic profile
        gender = random.choice(["Male", "Female", "Other", "Prefer not to say"])
        age = random.randint(18, 70)
        dob = fake.date_of_birth(minimum_age=age, maximum_age=age)
        
        # Use MySQL registration date if available
        if 'registration_date' in mysql_profile:
            signup_date = mysql_profile['registration_date'].date()
        else:
            signup_date = fake.date_between(start_date="-2y", end_date="today")
        
        # Customer tier from MySQL or generate
        customer_tier = mysql_profile.get('tier', random.choice(['bronze', 'silver', 'gold', 'platinum']))
        
        # Total spent based on tier and account age
        days_since_signup = (datetime.now().date() - signup_date).days
        base_spend = {
            'bronze': random.uniform(0, 500),
            'silver': random.uniform(300, 1500),
            'gold': random.uniform(1000, 5000),
            'platinum': random.uniform(3000, 20000)
        }.get(customer_tier, 0)
        
        # Adjust by account age
        total_spent = base_spend * (days_since_signup / 365) * random.uniform(0.5, 2.0)
        
        # 3% chance of data inconsistency
        if random.random() < 0.03:
            total_spent = random.uniform(-100, 0)  # Negative spending
        
        # Activity status from MySQL or generate
        is_active = mysql_profile.get('is_active', random.random() > 0.1)
        
        # Last login based on activity
        if is_active:
            last_login_days = random.randint(0, 30)
        else:
            last_login_days = random.randint(60, 365)
        
        timezone = random.choice(TIMEZONES)
        tz = pytz.timezone(timezone)
        last_login = tz.localize(
            datetime.now() - timedelta(days=last_login_days, hours=random.randint(0, 23))
        )
        
        # Location
        country = fake.country()
        city = fake.city()
        
        # Preferences
        preferred_language = random.choice(['en', 'vi', 'es', 'fr', 'de', 'ja', 'ko'])
        marketing_consent = random.random() > 0.3  # 70% consent
        
        # Phone number (80% have phone)
        phone = fake.phone_number() if random.random() > 0.2 else None
        
        # 2% chance of missing critical data
        if random.random() < 0.02:
            email = None
        else:
            email = mysql_profile.get('email', fake.email())
        
        user = {
            "user_id": uid,
            "name": fake.name(),
            "email": email,
            "gender": gender,
            "dob": dob,
            "country": country,
            "city": city,
            "signup_date": signup_date,
            "last_login": last_login,
            "is_active": is_active,
            "customer_tier": customer_tier,
            "total_spent": round(total_spent, 2),
            "preferred_language": preferred_language,
            "marketing_consent": marketing_consent,
            "phone": phone,
            "timezone": timezone
        }
        users.append(user)
    
    return users

def generate_enhanced_sessions():
    """Generate realistic user sessions"""
    sessions = []
    
    for _ in range(NUM_SESSIONS):
        user_id = random.choice(USER_IDS)
        session_id = str(uuid.uuid4())
        
        # Session timing
        timezone = random.choice(TIMEZONES)
        tz = pytz.timezone(timezone)
        
        start_time = tz.localize(fake.date_time_between(start_date="-3M", end_date="now"))
        
        # Session duration (realistic distribution)
        duration_minutes = random.choices(
            [1, 5, 15, 30, 60, 120, 240],
            weights=[10, 25, 30, 20, 10, 4, 1]
        )[0]
        
        # 2% chance of very long sessions (data quality issue)
        if random.random() < 0.02:
            duration_minutes = random.randint(500, 1440)  # 8+ hours
        
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        # Device and browser
        device_type = random.choice(DEVICE_TYPES)
        browser = random.choice(BROWSERS)
        os = random.choice(OS_TYPES)
        
        # Mobile users more likely to have shorter sessions
        if device_type == 'mobile' and duration_minutes > 60:
            duration_minutes = random.randint(5, 60)
            end_time = start_time + timedelta(minutes=duration_minutes)
        
        # Location
        ip_address = fake.ipv4()
        location_country = fake.country()
        location_city = fake.city()
        
        # Session activity
        total_interactions = random.randint(1, 50)
        pages_visited = random.randint(1, min(30, total_interactions))
        
        # Conversion events (10% of sessions)
        conversion_events = ['purchase', 'signup', 'newsletter_signup', 'contact_form', 'demo_request']
        conversion_event = random.choice(conversion_events) if random.random() < 0.1 else None
        
        # Exit page
        exit_pages = ['home', 'product_detail', 'cart', 'checkout', 'search', 'category']
        exit_page = random.choice(exit_pages)
        
        session = {
            "session_id": session_id,
            "user_id": user_id,
            "start_time": start_time,
            "end_time": end_time,
            "device_type": device_type,
            "browser": browser,
            "os": os,
            "ip_address": ip_address,
            "location_country": location_country,
            "location_city": location_city,
            "total_interactions": total_interactions,
            "pages_visited": pages_visited,
            "conversion_event": conversion_event,
            "exit_page": exit_page,
            "session_duration_minutes": duration_minutes
        }
        
        sessions.append(session)
    
    return sessions

def generate_enhanced_interactions():
    """Generate realistic user interactions with business logic"""
    interactions = []
    
    # Generate sessions first for context
    sessions = generate_enhanced_sessions()
    
    for _ in range(NUM_INTERACTIONS):
        user_id = random.choice(USER_IDS)
        
        # 30% chance to use existing session, 70% create new context
        if random.random() < 0.3 and sessions:
            session = random.choice([s for s in sessions if s['user_id'] == user_id])
            if session:
                session_id = session['session_id']
                device_type = session['device_type']
                browser = session['browser']
                os = session['os']
                ip_address = session['ip_address']
                
                # Timestamp within session
                timestamp = fake.date_time_between(
                    start_date=session['start_time'], 
                    end_date=session['end_time']
                )
            else:
                session_id = str(uuid.uuid4())
                device_type = random.choice(DEVICE_TYPES)
                browser = random.choice(BROWSERS)
                os = random.choice(OS_TYPES)
                ip_address = fake.ipv4()
                timestamp = fake.date_time_between(start_date="-3M", end_date="now")
        else:
            session_id = str(uuid.uuid4())
            device_type = random.choice(DEVICE_TYPES)
            browser = random.choice(BROWSERS)
            os = random.choice(OS_TYPES)
            ip_address = fake.ipv4()
            timestamp = fake.date_time_between(start_date="-3M", end_date="now")
        
        # Ensure timezone consistency
        timezone = random.choice(TIMEZONES)
        tz = pytz.timezone(timezone)
        if timestamp.tzinfo is None:
            timestamp = tz.localize(timestamp)
        
        interaction_type = random.choice(INTERACTION_TYPES)
        
        # Target ID based on interaction type
        if interaction_type in ['product_view', 'product_like', 'product_comment', 'product_rating', 'add_to_cart']:
            target_id = random.choice(PRODUCT_IDS)
        elif interaction_type == 'search':
            target_id = f"search_{random.randint(1, 1000)}"
        elif interaction_type == 'filter_apply':
            target_id = f"filter_{random.choice(['category', 'price', 'brand', 'rating'])}"
        else:
            target_id = f"general_{random.randint(1, 100)}"
        
        # Generate metadata based on interaction type
        metadata = {}
        duration_seconds = 0
        success = True
        
        if interaction_type == "product_view":
            metadata = {
                "scroll_depth": random.randint(10, 100),
                "time_on_page": random.randint(5, 300),
                "images_viewed": random.randint(1, 8),
                "zoom_used": random.choice([True, False])
            }
            duration_seconds = metadata["time_on_page"]
            
        elif interaction_type == "product_rating":
            metadata = {
                "rating": random.randint(1, 5),
                "verified_purchase": random.choice([True, False]),
                "review_length": random.randint(10, 500)
            }
            
        elif interaction_type == "search":
            search_terms = ["smartphone", "laptop", "shoes", "dress", "book", "headphones"]
            metadata = {
                "query": random.choice(search_terms),
                "results_count": random.randint(0, 1000),
                "clicked_position": random.randint(1, 10) if random.random() > 0.3 else None
            }
            
        elif interaction_type == "add_to_cart":
            metadata = {
                "quantity": random.randint(1, 5),
                "variant_id": str(uuid.uuid4()),
                "cart_total_items": random.randint(1, 10)
            }
            
        elif interaction_type == "purchase":
            metadata = {
                "order_total": round(random.uniform(20, 2000), 2),
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
                "items_count": random.randint(1, 8)
            }
            # Purchase interactions are always successful
            success = True
            
        elif interaction_type == "filter_apply":
            metadata = {
                "filter_type": random.choice(["category", "price", "brand", "rating"]),
                "filter_value": random.choice(["Electronics", "Fashion", "50-100", "4+stars"]),
                "results_after_filter": random.randint(0, 500)
            }
        
        # 3% chance of failed interactions
        if random.random() < 0.03:
            success = False
            metadata["error"] = "timeout"
        
        # 2% chance of missing metadata (data quality issue)
        if random.random() < 0.02:
            metadata = None
        
        # User agent string
        user_agent = fake.user_agent()
        
        # Referrer
        referrers = [
            "https://google.com",
            "https://facebook.com",
            "https://instagram.com",
            "direct",
            "https://twitter.com",
            None
        ]
        referrer = random.choice(referrers)
        
        interaction = {
            "interaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "type": interaction_type,
            "target_id": target_id,
            "timestamp": timestamp,
            "metadata": json.dumps(metadata) if metadata else None,
            "device_type": device_type,
            "browser": browser,
            "os": os,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "referrer": referrer,
            "duration_seconds": duration_seconds,
            "success": success
        }
        
        interactions.append(interaction)
    
    return interactions

def generate_user_preferences():
    """Generate user preferences based on interactions"""
    preferences = []
    
    categories = ['Electronics', 'Fashion', 'Books', 'Home & Garden', 'Sports', 'Beauty', 'Toys', 'Automotive']
    preference_types = ['category', 'brand', 'price_range', 'color', 'style', 'feature']
    
    for _ in range(NUM_PREFERENCES):
        user_id = random.choice(USER_IDS)
        
        pref_type = random.choice(preference_types)
        
        # Generate realistic preference values
        if pref_type == 'category':
            pref_value = random.choice(categories)
        elif pref_type == 'brand':
            pref_value = random.choice(['Apple', 'Samsung', 'Nike', 'Adidas', 'IKEA', 'Sony'])
        elif pref_type == 'price_range':
            pref_value = random.choice(['0-50', '50-100', '100-300', '300-1000', '1000+'])
        elif pref_type == 'color':
            pref_value = random.choice(['red', 'blue', 'green', 'black', 'white', 'multicolor'])
        elif pref_type == 'style':
            pref_value = random.choice(['modern', 'classic', 'vintage', 'minimalist', 'luxury'])
        else:
            pref_value = random.choice(['wireless', 'waterproof', 'fast', 'durable', 'eco-friendly'])
        
        # Confidence score (machine learning derived)
        confidence_score = random.uniform(0.1, 1.0)
        
        # Source of preference
        source = random.choice(['purchase_history', 'browsing_behavior', 'explicit_feedback', 'ml_prediction'])
        
        # Explicit preferences are more confident
        is_explicit = random.random() < 0.2
        if is_explicit:
            confidence_score = random.uniform(0.8, 1.0)
        
        preference = {
            "preference_id": str(uuid.uuid4()),
            "user_id": user_id,
            "category": random.choice(categories),
            "preference_type": pref_type,
            "preference_value": pref_value,
            "confidence_score": round(confidence_score, 3),
            "last_updated": fake.date_time_between(start_date="-6M", end_date="now"),
            "source": source,
            "is_explicit": is_explicit
        }
        
        preferences.append(preference)
    
    return preferences

def generate_user_notifications():
    """Generate user notifications"""
    notifications = []
    
    notification_types = ['order_update', 'promotion', 'product_recommendation', 'price_drop', 'restock', 'review_request', 'newsletter', 'account_security']
    channels = ['email', 'push', 'sms', 'in_app']
    priorities = ['low', 'medium', 'high', 'urgent']
    
    for _ in range(NUM_NOTIFICATIONS):
        user_id = random.choice(USER_IDS)
        notif_type = random.choice(notification_types)
        
        # Generate realistic notification content
        if notif_type == 'order_update':
            title = "Order Update"
            content = f"Your order ORD-{random.randint(100000, 999999)} has been {random.choice(['confirmed', 'shipped', 'delivered'])}"
            priority = 'high'
        elif notif_type == 'promotion':
            title = "Special Offer"
            content = f"Get {random.randint(10, 50)}% off on {random.choice(['Electronics', 'Fashion', 'Books'])}"
            priority = 'medium'
        elif notif_type == 'product_recommendation':
            title = "You might like this"
            content = "Based on your browsing history, we found products you might love"
            priority = 'low'
        elif notif_type == 'price_drop':
            title = "Price Drop Alert"
            content = f"Item in your wishlist is now ${random.randint(50, 500)} cheaper"
            priority = 'medium'
        elif notif_type == 'restock':
            title = "Back in Stock"
            content = "The item you wanted is now available"
            priority = 'high'
        elif notif_type == 'review_request':
            title = "How was your purchase?"
            content = "Please share your experience with recent purchase"
            priority = 'low'
        elif notif_type == 'newsletter':
            title = "Weekly Newsletter"
            content = "Check out this week's trends and new arrivals"
            priority = 'low'
        else:  # account_security
            title = "Security Alert"
            content = "New login detected from unknown device"
            priority = 'urgent'
        
        # Send timestamp
        sent_at = fake.date_time_between(start_date="-3M", end_date="now")
        
        # Read and click timestamps (realistic behavior)
        read_at = None
        clicked_at = None
        
        # 70% of notifications are read
        if random.random() < 0.7:
            read_at = sent_at + timedelta(
                hours=random.randint(0, 48),
                minutes=random.randint(0, 59)
            )
            
            # 30% of read notifications are clicked
            if random.random() < 0.3:
                clicked_at = read_at + timedelta(
                    minutes=random.randint(1, 30)
                )
        
        # Channel preference
        channel = random.choice(channels)
        
        # Category
        category = random.choice(['transactional', 'marketing', 'system', 'social'])
        
        # Metadata
        metadata = {
            "campaign_id": str(uuid.uuid4()) if notif_type in ['promotion', 'newsletter'] else None,
            "product_id": random.choice(PRODUCT_IDS) if notif_type in ['product_recommendation', 'price_drop', 'restock'] else None,
            "personalization_score": random.uniform(0.1, 1.0),
            "a_b_test_group": random.choice(['A', 'B', 'control']) if notif_type == 'promotion' else None
        }
        
        notification = {
            "notification_id": str(uuid.uuid4()),
            "user_id": user_id,
            "type": notif_type,
            "title": title,
            "content": content,
            "sent_at": sent_at,
            "read_at": read_at,
            "clicked_at": clicked_at,
            "channel": channel,
            "priority": priority,
            "category": category,
            "metadata": json.dumps(metadata)
        }
        
        notifications.append(notification)
    
    return notifications

# ---------- INSERT INTO POSTGRES ----------
def insert_to_postgres(table, records):
    """Insert records into PostgreSQL with batch processing"""
    if not records:
        return
    
    batch_size = 1000
    with engine.connect() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            conn.execute(table.insert(), batch)
            conn.commit()

# ---------- MAIN ----------
def main():
    """Main execution function"""
    print("=" * 60)
    print("🚀 ENHANCED USER DATA GENERATOR FOR POSTGRESQL")
    print("=" * 60)
    
    try:
        # Test database connection
        print("\n🔧 Testing database connection...")
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        print("✅ Database connection successful!")
        
        # Load shared data
        print("\n📊 Loading shared data...")
        print(f"Found {len(USER_IDS)} users and {len(PRODUCT_IDS)} products")
        
        # Generate data
        print("\n📝 Generating enhanced users...")
        users = generate_enhanced_users()
        
        print("🔄 Generating user sessions...")
        sessions = generate_enhanced_sessions()
        
        print("👆 Generating user interactions...")
        interactions = generate_enhanced_interactions()
        
        print("💡 Generating user preferences...")
        preferences = generate_user_preferences()
        
        print("📨 Generating user notifications...")
        notifications = generate_user_notifications()
        
        # Insert data with progress tracking
        print("\n💾 Inserting data into PostgreSQL...")
        
        print("  🔸 Inserting users...")
        insert_to_postgres(users_table, users)
        print(f"  ✅ Inserted {len(users)} users")
        
        print("  🔸 Inserting sessions...")
        insert_to_postgres(sessions_table, sessions)
        print(f"  ✅ Inserted {len(sessions)} sessions")
        
        print("  🔸 Inserting interactions...")
        insert_to_postgres(interactions_table, interactions)
        print(f"  ✅ Inserted {len(interactions)} interactions")
        
        print("  🔸 Inserting preferences...")
        insert_to_postgres(preferences_table, preferences)
        print(f"  ✅ Inserted {len(preferences)} preferences")
        
        print("  🔸 Inserting notifications...")
        insert_to_postgres(notifications_table, notifications)
        print(f"  ✅ Inserted {len(notifications)} notifications")
        
        # Generate comprehensive report
        print("\n📋 Generating data quality report...")
        generate_data_quality_report(users, sessions, interactions, preferences, notifications)
        
        print("\n" + "=" * 60)
        print("🎉 ALL DATA INSERTED SUCCESSFULLY!")
        print("=" * 60)
        
        # Display final statistics
        print("\n📊 FINAL STATISTICS:")
        print(f"  • Total Users: {len(users):,}")
        print(f"  • Total Sessions: {len(sessions):,}")
        print(f"  • Total Interactions: {len(interactions):,}")
        print(f"  • Total Preferences: {len(preferences):,}")
        print(f"  • Total Notifications: {len(notifications):,}")
        print(f"  • Total Records: {len(users) + len(sessions) + len(interactions) + len(preferences) + len(notifications):,}")
        
        print("\n📁 Generated files:")
        print("  • user_data_quality_report.json")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def generate_data_quality_report(users, sessions, interactions, preferences, notifications):
    """Generate comprehensive data quality report"""
    
    print("  📊 Analyzing data quality...")
    
    # Calculate metrics
    users_with_missing_email = sum(1 for u in users if u.get("email") is None)
    users_with_negative_spending = sum(1 for u in users if u.get("total_spent", 0) < 0)
    inactive_users = sum(1 for u in users if not u.get("is_active", True))
    
    sessions_very_long = sum(1 for s in sessions if s.get("session_duration_minutes", 0) > 480)
    sessions_with_conversions = sum(1 for s in sessions if s.get("conversion_event") is not None)
    
    failed_interactions = sum(1 for i in interactions if not i.get("success", True))
    interactions_missing_metadata = sum(1 for i in interactions if i.get("metadata") is None)
    
    low_confidence_preferences = sum(1 for p in preferences if p.get("confidence_score", 0) < 0.3)
    explicit_preferences = sum(1 for p in preferences if p.get("is_explicit", False))
    
    unread_notifications = sum(1 for n in notifications if n.get("read_at") is None)
    clicked_notifications = sum(1 for n in notifications if n.get("clicked_at") is not None)
    
    # Additional metrics
    total_user_spending = sum(u.get("total_spent", 0) for u in users)
    avg_user_spending = total_user_spending / len(users) if users else 0
    
    total_session_duration = sum(s.get("session_duration_minutes", 0) for s in sessions)
    avg_session_duration = total_session_duration / len(sessions) if sessions else 0
    
    # Device type distribution
    device_distribution = {}
    for session in sessions:
        device = session.get("device_type", "unknown")
        device_distribution[device] = device_distribution.get(device, 0) + 1
    
    # Interaction type distribution
    interaction_distribution = {}
    for interaction in interactions:
        int_type = interaction.get("type", "unknown")
        interaction_distribution[int_type] = interaction_distribution.get(int_type, 0) + 1
    
    # Notification channel distribution
    notification_channels = {}
    for notification in notifications:
        channel = notification.get("channel", "unknown")
        notification_channels[channel] = notification_channels.get(channel, 0) + 1
    
    # Customer tier distribution
    tier_distribution = {}
    for user in users:
        tier = user.get("customer_tier", "unknown")
        tier_distribution[tier] = tier_distribution.get(tier, 0) + 1
    
    # Create comprehensive report
    report = {
        "generation_timestamp": datetime.now().isoformat(),
        "summary": {
            "total_users": len(users),
            "total_sessions": len(sessions),
            "total_interactions": len(interactions),
            "total_preferences": len(preferences),
            "total_notifications": len(notifications)
        },
        "data_quality_issues": {
            "users": {
                "missing_email": users_with_missing_email,
                "missing_email_percentage": round((users_with_missing_email / len(users)) * 100, 2) if users else 0,
                "negative_spending": users_with_negative_spending,
                "negative_spending_percentage": round((users_with_negative_spending / len(users)) * 100, 2) if users else 0,
                "inactive_users": inactive_users,
                "inactive_percentage": round((inactive_users / len(users)) * 100, 2) if users else 0
            },
            "sessions": {
                "very_long_sessions": sessions_very_long,
                "very_long_percentage": round((sessions_very_long / len(sessions)) * 100, 2) if sessions else 0,
                "sessions_with_conversions": sessions_with_conversions,
                "conversion_rate": round((sessions_with_conversions / len(sessions)) * 100, 2) if sessions else 0
            },
            "interactions": {
                "failed_interactions": failed_interactions,
                "failure_rate": round((failed_interactions / len(interactions)) * 100, 2) if interactions else 0,
                "missing_metadata": interactions_missing_metadata,
                "missing_metadata_percentage": round((interactions_missing_metadata / len(interactions)) * 100, 2) if interactions else 0
            },
            "preferences": {
                "low_confidence": low_confidence_preferences,
                "low_confidence_percentage": round((low_confidence_preferences / len(preferences)) * 100, 2) if preferences else 0,
                "explicit_preferences": explicit_preferences,
                "explicit_percentage": round((explicit_preferences / len(preferences)) * 100, 2) if preferences else 0
            },
            "notifications": {
                "unread_notifications": unread_notifications,
                "unread_percentage": round((unread_notifications / len(notifications)) * 100, 2) if notifications else 0,
                "clicked_notifications": clicked_notifications,
                "click_rate": round((clicked_notifications / len(notifications)) * 100, 2) if notifications else 0
            }
        },
        "business_metrics": {
            "total_user_spending": round(total_user_spending, 2),
            "average_user_spending": round(avg_user_spending, 2),
            "average_session_duration_minutes": round(avg_session_duration, 2),
            "customer_tier_distribution": tier_distribution,
            "device_type_distribution": device_distribution,
            "interaction_type_distribution": interaction_distribution,
            "notification_channel_distribution": notification_channels
        },
        "data_freshness": {
            "users_last_30_days": sum(1 for u in users if u.get("last_login") and 
                                    (datetime.now(pytz.UTC) - u["last_login"]).days <= 30),
            "sessions_last_7_days": sum(1 for s in sessions if s.get("start_time") and 
                                      (datetime.now(pytz.UTC) - s["start_time"]).days <= 7),
            "interactions_last_24_hours": sum(1 for i in interactions if i.get("timestamp") and 
                                            (datetime.now(pytz.UTC) - i["timestamp"]).total_seconds() <= 86400)
        },
        "recommendations": []
    }
    
    # Generate recommendations based on data quality issues
    if users_with_missing_email > 0:
        report["recommendations"].append({
            "type": "data_quality",
            "priority": "high",
            "issue": "Missing email addresses",
            "description": f"{users_with_missing_email} users have missing email addresses",
            "suggestion": "Implement email validation and collection processes"
        })
    
    if users_with_negative_spending > 0:
        report["recommendations"].append({
            "type": "data_quality",
            "priority": "medium",
            "issue": "Negative spending values",
            "description": f"{users_with_negative_spending} users have negative spending amounts",
            "suggestion": "Review refund and return processing logic"
        })
    
    if sessions_very_long > 0:
        report["recommendations"].append({
            "type": "data_quality",
            "priority": "low",
            "issue": "Unusually long sessions",
            "description": f"{sessions_very_long} sessions are longer than 8 hours",
            "suggestion": "Implement session timeout and cleanup mechanisms"
        })
    
    if failed_interactions / len(interactions) > 0.05:  # More than 5% failure rate
        report["recommendations"].append({
            "type": "performance",
            "priority": "high",
            "issue": "High interaction failure rate",
            "description": f"{round((failed_interactions / len(interactions)) * 100, 2)}% of interactions are failing",
            "suggestion": "Investigate and fix interaction failure causes"
        })
    
    if unread_notifications / len(notifications) > 0.5:  # More than 50% unread
        report["recommendations"].append({
            "type": "engagement",
            "priority": "medium",
            "issue": "Low notification engagement",
            "description": f"{round((unread_notifications / len(notifications)) * 100, 2)}% of notifications are unread",
            "suggestion": "Review notification timing, content, and channels"
        })
    
    # Save report to file
    with open('user_data_quality_report.json', 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    # Print summary
    print("  ✅ Data quality report generated")
    print(f"  📊 Users with issues: {users_with_missing_email + users_with_negative_spending}")
    print(f"  ⚠️  Failed interactions: {failed_interactions} ({round((failed_interactions / len(interactions)) * 100, 1)}%)")
    print(f"  📱 Most used device: {max(device_distribution.items(), key=lambda x: x[1])[0] if device_distribution else 'N/A'}")
    print(f"  💰 Total revenue: ${round(total_user_spending, 2):,}")
    print(f"  📈 Conversion rate: {round((sessions_with_conversions / len(sessions)) * 100, 2)}%")
    
    return report