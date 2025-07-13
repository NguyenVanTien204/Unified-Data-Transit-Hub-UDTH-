import random
import uuid
from datetime import datetime, timedelta
import pytz
import re
from faker import Faker
from sqlalchemy import (
    create_engine, Column, String, Float, DateTime, Enum, ForeignKey, Integer, Boolean, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import json

# --- CONFIG ---
fake = Faker()
Base = declarative_base()
DB_URI = "mysql+pymysql://root:141124@localhost:3306/ordersdb"  # ← Update
TIMEZONES = ["UTC", "Asia/Ho_Chi_Minh", "Europe/London", "America/New_York", "Asia/Tokyo"]
NUM_USERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 200

# Shared data for synchronization
SHARED_DATA = {
    'user_ids': [],
    'product_ids': [],
    'user_profiles': {}
}

def clean_phone_number():
    raw = fake.phone_number()
    digits = re.sub(r'\D', '', raw)
    return digits[:10]  # Hoặc 9 nếu muốn

# --- SCHEMA ---
class User(Base):
    __tablename__ = 'users'
    user_id = Column(String(36), primary_key=True)
    email = Column(String(100), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    phone = Column(String(20))
    address = Column(Text)
    city = Column(String(50))
    country = Column(String(50))
    registration_date = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True)
    customer_tier = Column(Enum('bronze', 'silver', 'gold', 'platinum', name='customer_tier'), default='bronze')

class Product(Base):
    __tablename__ = 'products'
    product_id = Column(String(36), primary_key=True)
    name = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    category = Column(String(50))
    brand = Column(String(50))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Order(Base):
    __tablename__ = 'orders'
    order_id = Column(String(36), primary_key=True)
    user_id = Column(String(36), ForeignKey('users.user_id'), nullable=True)  # Allow null for guest orders
    order_date = Column(DateTime, nullable=False)
    status = Column(Enum('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded', name='order_status'), nullable=False)
    total_amount = Column(Float, nullable=False)
    shipping_fee = Column(Float, default=0.0)
    tax_amount = Column(Float, default=0.0)
    discount_amount = Column(Float, default=0.0)
    payment_status = Column(Enum('pending', 'paid', 'failed', 'refunded', name='payment_status'), default='pending')
    shipping_address = Column(Text)
    notes = Column(Text)
    currency = Column(String(3), default='USD')

    user = relationship("User", backref="orders")
    items = relationship("OrderItem", back_populates="order")
    transactions = relationship("Transaction", back_populates="order")

class OrderItem(Base):
    __tablename__ = 'order_items'
    item_id = Column(String(36), primary_key=True)
    order_id = Column(String(36), ForeignKey('orders.order_id'), nullable=False)
    product_id = Column(String(36), ForeignKey('products.product_id'), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    discount_per_item = Column(Float, default=0.0)
    total_price = Column(Float, nullable=False)

    order = relationship("Order", back_populates="items")
    product = relationship("Product", backref="order_items")

class Transaction(Base):
    __tablename__ = 'transactions'
    transaction_id = Column(String(36), primary_key=True)
    order_id = Column(String(36), ForeignKey('orders.order_id'), nullable=False)
    amount = Column(Float, nullable=False)
    method = Column(Enum('credit_card', 'paypal', 'bank_transfer', 'cash_on_delivery', 'wallet', name='payment_method'), nullable=False)
    status = Column(Enum('pending', 'processing', 'success', 'failed', 'cancelled', name='transaction_status'), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    gateway_response = Column(Text)  # JSON response from payment gateway
    reference_number = Column(String(50))
    fee = Column(Float, default=0.0)

    order = relationship("Order", back_populates="transactions")

# --- DB SETUP ---
engine = create_engine(DB_URI)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# --- GENERATORS ---
def generate_users():
    """Generate realistic user data"""
    users = []
    emails = set()
    
    for _ in range(NUM_USERS):
        user_id = str(uuid.uuid4())
        
        # Ensure unique email
        while True:
            email = fake.email()
            if email not in emails:
                emails.add(email)
                break
        
        # Some users might be inactive (5%)
        is_active = random.random() > 0.05
        
        # Customer tier based on registration date (older users tend to have higher tier)
        reg_date = fake.date_time_between(start_date="-2y", end_date="now")
        days_since_reg = (datetime.now() - reg_date).days
        
        if days_since_reg > 730:  # 2 years
            tier = random.choices(['bronze', 'silver', 'gold', 'platinum'], weights=[2, 3, 3, 2])[0]
        elif days_since_reg > 365:  # 1 year
            tier = random.choices(['bronze', 'silver', 'gold', 'platinum'], weights=[3, 4, 2, 1])[0]
        else:
            tier = random.choices(['bronze', 'silver', 'gold', 'platinum'], weights=[6, 3, 1, 0])[0]
        
        user = User(
            user_id=user_id,
            email=email,
            name=fake.name(),
            phone=clean_phone_number() if random.random() > 0.1 else None,
            address=fake.address(),
            city=fake.city(),
            country=fake.country(),
            registration_date=reg_date,
            is_active=is_active,
            customer_tier=tier
        )
        users.append(user)
        print(f"✅Generated user: {user.name}, Email: {email}, Tier: {tier}, Active: {is_active}, Phone: {user.phone}, Address: {user.address}, City: {user.city}, Country: {user.country}, Registration Date: {user.registration_date}")
        # Store for synchronization
        SHARED_DATA['user_ids'].append(user_id)
        SHARED_DATA['user_profiles'][user_id] = {
            'tier': tier,
            'registration_date': reg_date,
            'is_active': is_active,
            'email': email
        }
    
    return users

def generate_products():
    """Generate realistic product data"""
    products = []
    categories = ['Electronics', 'Fashion', 'Books', 'Home & Garden', 'Sports', 'Beauty', 'Toys', 'Automotive']
    brands = ['Apple', 'Samsung', 'Nike', 'Adidas', 'IKEA', 'Sony', 'Generic', 'NoName']
    
    for _ in range(NUM_PRODUCTS):
        product_id = str(uuid.uuid4())
        category = random.choice(categories)
        brand = random.choice(brands)
        
        # Price varies by category
        if category == 'Electronics':
            price = round(random.uniform(50, 2000), 2)
        elif category == 'Fashion':
            price = round(random.uniform(20, 500), 2)
        elif category == 'Books':
            price = round(random.uniform(10, 50), 2)
        else:
            price = round(random.uniform(15, 300), 2)
        
        # 2% chance of pricing errors
        if random.random() < 0.02:
            price = round(random.uniform(-50, 0), 2)  # Negative price error
        
        # 3% chance product is discontinued
        is_active = random.random() > 0.03
        
        product = Product(
            product_id=product_id,
            name=fake.catch_phrase(),
            price=price,
            category=category,
            brand=brand,
            is_active=is_active,
            created_at=fake.date_time_between(start_date="-1y", end_date="now")
        )
        products.append(product)
        SHARED_DATA['product_ids'].append(product_id)
    
    return products

def generate_order():
    """Generate realistic order with business logic"""
    order_id = str(uuid.uuid4())
    
    # 8% chance of guest order (no user_id)
    user_id = random.choice(SHARED_DATA['user_ids']) if random.random() > 0.08 else None
    
    # Order date with realistic timezone
    timezone = pytz.timezone(random.choice(TIMEZONES))
    naive_time = fake.date_time_between(start_date="-6M", end_date="now")
    order_date = timezone.localize(naive_time)
    
    # Status distribution with business logic
    # Most orders should be completed, some pending, few cancelled
    status_weights = {
        'pending': 5,
        'confirmed': 3,
        'processing': 4,
        'shipped': 8,
        'delivered': 65,
        'cancelled': 10,
        'refunded': 5
    }
    status = random.choices(list(status_weights.keys()), weights=list(status_weights.values()))[0]
    
    # Payment status should correlate with order status
    if status in ['delivered', 'shipped', 'processing']:
        payment_status = 'paid'
    elif status == 'cancelled':
        payment_status = random.choice(['failed', 'refunded'])
    elif status == 'refunded':
        payment_status = 'refunded'
    else:
        payment_status = random.choice(['pending', 'paid'])
    
    # Currency based on timezone (simplified)
    currency_map = {
        'UTC': 'USD',
        'Asia/Ho_Chi_Minh': 'VND',
        'Europe/London': 'GBP',
        'America/New_York': 'USD',
        'Asia/Tokyo': 'JPY'
    }
    currency = currency_map.get(timezone.zone, 'USD')
    
    return Order(
        order_id=order_id,
        user_id=user_id,
        order_date=order_date,
        status=status,
        total_amount=0.0,  # Will be calculated
        payment_status=payment_status,
        shipping_address=fake.address(),
        notes=fake.text(max_nb_chars=200) if random.random() > 0.7 else None,
        currency=currency
    )

def generate_items(order, products):
    """Generate order items with realistic business logic"""
    n = random.randint(1, 5)
    total_amount = 0.0
    shipping_fee = 0.0
    tax_amount = 0.0
    discount_amount = 0.0
    items = []
    
    # Filter active products
    active_products = [p for p in products if p.is_active]
    if not active_products:
        active_products = products[:10]  # Fallback
    
    selected_products = random.sample(active_products, min(n, len(active_products)))
    
    for product in selected_products:
        quantity = random.randint(1, 3)
        if random.random() < 0.01:
            quantity = random.choice([0, -1, 100])  # intentionally bad

        unit_price = product.price if product.price is not None else random.uniform(5, 50)
        if random.random() < 0.02:
            unit_price = random.choice([None, 0.0, -5.0])  # simulate pricing errors

        if unit_price is None or quantity is None or quantity <= 0:
            # fallback logic: random total_price but logically valid
            fallback_price = random.uniform(10, 100)
            total_price = fallback_price
            discount_per_item = 0.0
            unit_price = fallback_price / max(quantity, 1)  # fake it
        else:
            # safe calculation
            discount_per_item = 0.0
            if random.random() < 0.05:
                discount_per_item = round(unit_price * random.uniform(0.1, 0.3), 2)
            total_price = max(0, (unit_price - discount_per_item) * quantity)

        total_amount += total_price
        discount_amount += discount_per_item * quantity if quantity else 0

        item = OrderItem(
            item_id=str(uuid.uuid4()),
            order_id=order.order_id,
            product_id=product.product_id,
            quantity=quantity,
            unit_price=unit_price,
            discount_per_item=discount_per_item,
            total_price=total_price
        )
        items.append(item)
    
    # Calculate shipping fee (free for orders > $100)
    if total_amount > 100:
        shipping_fee = 0.0
    else:
        shipping_fee = round(random.uniform(5, 25), 2)
    
    # Calculate tax (8-12% depending on location)
    tax_rate = random.uniform(0.08, 0.12)
    tax_amount = round(total_amount * tax_rate, 2)
    
    # Update order totals
    order.total_amount = round(total_amount + shipping_fee + tax_amount, 2)
    order.shipping_fee = shipping_fee
    order.tax_amount = tax_amount
    order.discount_amount = discount_amount
    
    return items

def generate_transactions(order):
    """Generate realistic payment transactions"""
    transactions = []
    remaining_amount = order.total_amount
    
    # No transactions for cancelled orders without payment
    if order.status == 'cancelled' and order.payment_status != 'refunded':
        return transactions
    
    # Payment method preference by user tier (if user exists)
    user_profile = SHARED_DATA['user_profiles'].get(order.user_id)
    if user_profile and user_profile['tier'] in ['gold', 'platinum']:
        payment_methods = ['credit_card', 'paypal', 'wallet']
        method_weights = [5, 3, 2]
    else:
        payment_methods = ['credit_card', 'paypal', 'bank_transfer', 'cash_on_delivery', 'wallet']
        method_weights = [4, 2, 2, 1, 1]
    
    # Generate payment attempts
    max_attempts = random.randint(1, 3)
    for attempt in range(max_attempts):
        method = random.choices(payment_methods, weights=method_weights)[0]
        
        # Transaction amount
        if attempt == 0:
            amount = remaining_amount
        else:
            amount = round(random.uniform(10, remaining_amount), 2)
        
        # Status based on order payment status
        if order.payment_status == 'paid':
            status = 'success' if attempt == max_attempts - 1 else 'failed'
        elif order.payment_status == 'failed':
            status = 'failed'
        elif order.payment_status == 'refunded':
            status = 'success'  # Original payment was successful
            # Add refund transaction
            refund_txn = Transaction(
                transaction_id=str(uuid.uuid4()),
                order_id=order.order_id,
                amount=-amount,  # Negative amount for refund
                method=method,
                status='success',
                timestamp=order.order_date + timedelta(days=random.randint(1, 30)),
                gateway_response=json.dumps({"type": "refund", "reason": "customer_request"}),
                reference_number=f"REF-{random.randint(100000, 999999)}",
                fee=round(amount * 0.03, 2)
            )
            transactions.append(refund_txn)
        else:
            status = random.choice(['pending', 'processing'])
        
        # Transaction timestamp
        if attempt == 0:
            timestamp = order.order_date + timedelta(minutes=random.randint(5, 60))
        else:
            timestamp = order.order_date + timedelta(minutes=random.randint(1, 30) + attempt * 15)
        
        # Gateway response simulation
        gateway_response = {
            "gateway": "stripe" if method == "credit_card" else method,
            "response_code": "00" if status == "success" else "05",
            "message": "approved" if status == "success" else "declined"
        }
        
        transaction = Transaction(
            transaction_id=str(uuid.uuid4()),
            order_id=order.order_id,
            amount=amount,
            method=method,
            status=status,
            timestamp=timestamp,
            gateway_response=json.dumps(gateway_response),
            reference_number=f"TXN-{random.randint(100000, 999999)}",
            fee=round(amount * 0.025, 2) if status == 'success' else 0.0
        )
        transactions.append(transaction)
        
        if status == 'success':
            remaining_amount -= amount
            break
    
    return transactions

# --- GENERATE & INSERT ---
def main():
    print("Generating users...")
    users = generate_users()
    session.add_all(users)
    session.commit()
    print(f"[✓] Inserted {len(users)} users")
    
    print("Generating products...")
    products = generate_products()
    session.add_all(products)
    session.commit()
    print(f"[✓] Inserted {len(products)} products")
    
    print("Generating orders and related data...")
    orders = []
    all_items = []
    all_transactions = []
    
    for i in range(NUM_ORDERS):
        if (i + 1) % 50 == 0:
            print(f"  Generated {i + 1}/{NUM_ORDERS} orders...")
        
        order = generate_order()
        items = generate_items(order, products)
        transactions = generate_transactions(order)
        
        orders.append(order)
        all_items.extend(items)
        all_transactions.extend(transactions)
    
    session.add_all(orders + all_items + all_transactions)
    session.commit()
    session.close()
    
    print(f"[✓] Inserted {len(orders)} orders, {len(all_items)} items, and {len(all_transactions)} transactions")
    
    # Save shared data for other scripts
    with open('shared_data.json', 'w') as f:
        json.dump({
            'user_ids': SHARED_DATA['user_ids'],
            'product_ids': SHARED_DATA['product_ids']
        }, f, indent=2)
    
    print("[✓] Shared data saved to shared_data.json")

if __name__ == "__main__":
    main()