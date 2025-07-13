# Table Descriptions
## User profile gồm các trường:
- user_id (UUID hoặc int)
- name
- email
- gender
- dob (ngày sinh)
- country
- signup_date

## User interaction gồm:
- interaction_id
- user_id (foreign key)
- type (view, like, comment, rating)
- target_id (ví dụ movie_id, blog_id, v.v.)
- timestamp
- metadata (dict kiểu JSON string - ví dụ rating value, comment content...)

# Orders
| Column        | Type     | Ghi chú                   |
| ------------- | -------- | ------------------------- |
| order\_id     | UUID     |                           |
| user\_id      | UUID     | Foreign key từ bảng users |
| order\_date   | DATETIME |                           |
| status        | ENUM     | pending / paid / failed   |
| total\_amount | FLOAT    | Tổng tiền hàng            |

# transactions
| Column          | Type     | Ghi chú                       |
| --------------- | -------- | ----------------------------- |
| transaction\_id | UUID     |                               |
| order\_id       | UUID     | Foreign key                   |
| amount          | FLOAT    | Có thể nhỏ hơn `total_amount` |
| method          | ENUM     | credit\_card, paypal, etc.    |
| status          | ENUM     | success / failed              |
| timestamp       | DATETIME |                               |


| Collection      | Nội dung chính                       | Phức tạp hóa bằng                              |
| --------------- | ------------------------------------ | ---------------------------------------------- |
| `products`      | Thông tin sản phẩm                   | Mảng biến thể (variants), specs dạng key-value |
| `stock_levels`  | Tồn kho tại nhiều kho hàng           | Lồng object theo từng `warehouse_id`           |
| `user_messages` | Tin nhắn giữa người dùng với support | Threading / reply chain                        |
| `ratings`       | Người dùng đánh giá sản phẩm         | Metadata như time, verified, tags              |
