# 🚀 Deliver AAU - Campus Delivery Telegram Bot

A bilingual (English + Amharic) Telegram bot ecosystem for campus delivery services at Addis Ababa University.

## 📋 Features

### ✨ Core Functionality
- **Bilingual Support**: Full English & Amharic localization
- **Onboarding Flow**: Language selection, phone verification, campus selection
- **Delivery Requests**: Complete delivery request flow with confirmations
- **Order Tracking**: Track active orders with real-time status updates
- **Gamification**: XP, coins, levels, and leaderboard system
- **User Profiles**: View and manage profile settings

### 🎮 Gamification System
- Earn **50 XP** and **10 coins** per delivery
- Level up every **100 XP**
- Bonus rewards for rating deliveries
- Top 10 leaderboard

### 🏫 Supported Campuses
- Main Campus (4 Kilo)
- Engineering Campus (6 Kilo)
- Technology Campus
- Lideta Campus
- Medical Campus

## 🏗️ Architecture

### Tech Stack
- **Framework**: aiogram v3 (async)
- **Database**: SQLite with aiosqlite
- **Scheduler**: APScheduler
- **Language**: Python 3.11+

### Project Structure
```
deliver_aau/
├── bot.py                  # Main entry point
├── config.py               # Configuration & constants
├── database/
│   └── db.py              # Async database operations
├── handlers/
│   ├── onboarding.py      # Registration flow
│   ├── student.py         # Main features
│   └── fallback.py        # Error handling
├── keyboards/
│   ├── reply.py           # Reply keyboards
│   └── inline.py          # Inline buttons
├── middlewares/
│   ├── logging_middleware.py
│   ├── throttling_middleware.py
│   └── language_middleware.py
├── utils/
│   ├── localization.py    # Bilingual messages
│   ├── helpers.py         # Utility functions
│   └── scheduler.py       # Background jobs
└── docker/
    ├── Dockerfile
    └── render.yaml
```

## 🚀 Quick Start

### Local Development

1. **Clone and Install**
```bash
cd deliver_aau
pip install -r requirements.txt
```

2. **Configure Environment**
```bash
# Edit .env file
BOT_TOKEN=your_telegram_bot_token
ADMIN_IDS=your_telegram_user_id
DB_PATH=deliver_aau.db
```

3. **Run Bot**
```bash
python bot.py
```

### Docker Deployment

1. **Build Image**
```bash
docker build -f docker/Dockerfile -t deliver-aau-bot .
```

2. **Run Container**
```bash
docker run -d \
  --name deliver-aau \
  -e BOT_TOKEN=your_token \
  -e ADMIN_IDS=123456789 \
  -v $(pwd)/data:/app/data \
  deliver-aau-bot
```

### Deploy to Render

1. Connect your GitHub repository to Render
2. Use `docker/render.yaml` configuration
3. Set environment variables in Render dashboard:
   - `BOT_TOKEN`
   - `ADMIN_IDS`
4. Deploy automatically on push

## 🎯 Bot Commands

- `/start` - Start bot / Show main menu

## 📱 User Flow

### First-Time Users
1. Select language (English/Amharic)
2. Share phone number
3. Select campus
4. Registration complete (earn 50 XP + 10 coins)

### Main Menu Options
- **📦 Request Delivery** - Create new delivery request
- **🚴‍♂️ Track Order** - View active orders
- **💰 My Coins** - Check balance and level
- **🏆 Leaderboard** - See top deliverers
- **🛠 Settings** - Update preferences

## 🔧 Configuration

### Environment Variables
| Variable | Description | Required |
|----------|-------------|----------|
| `BOT_TOKEN` | Telegram Bot API token | Yes |
| `ADMIN_IDS` | Comma-separated admin user IDs | Yes |
| `DB_PATH` | SQLite database file path | No (default: deliver_aau.db) |

### Gamification Constants (config.py)
```python
XP_PER_DELIVERY = 50
COINS_PER_DELIVERY = 10
XP_FOR_LEVEL_UP = 100
```

### Rate Limiting (config.py)
```python
RATE_LIMIT = 3  # messages per time window
TIME_WINDOW = 2  # seconds
```

## 🔄 Background Jobs

Automated tasks via APScheduler:
- **Daily Leaderboard Reset**: Midnight (00:00)
- **Admin Summary**: 23:00 daily
- **Inactive Cleanup**: Every 6 hours

## 📊 Database Schema

### Users Table
- `user_id` (PRIMARY KEY)
- `username`, `first_name`, `last_name`
- `phone`, `campus`, `language`
- `xp`, `coins`, `level`
- `total_deliveries`
- `created_at`, `last_active`

### Orders Table
- `order_id` (PRIMARY KEY)
- `user_id` (FOREIGN KEY)
- `pickup_location`, `dropoff_location`
- `item_description`, `status`
- `courier_id`, `rating`
- `created_at`, `updated_at`

### Onboarding State Table
- `user_id` (PRIMARY KEY)
- `step`, `data`

## 🛡️ Security Features

- **Rate Limiting**: Prevents spam (3 messages per 2 seconds)
- **Input Validation**: All user inputs validated
- **Graceful Error Handling**: No crashes on invalid input
- **Logging**: All updates logged with timestamps

## 🌐 Localization

Full bilingual support for all messages:
- Onboarding flow
- Menu buttons
- Notifications
- Error messages

Add new languages by extending `utils/localization.py`.

## 📝 Development Notes

### Adding New Features
1. Create handler in `handlers/`
2. Register router in `bot.py`
3. Add localized messages in `utils/localization.py`
4. Create keyboards in `keyboards/`

### Database Migrations
Modify `database/db.py` `_create_tables()` method for schema changes.

### Type Hints
All functions include full type hints and docstrings.

### Code Style
- PEP8 compliant
- Async-first architecture
- Modular design

## 🐛 Troubleshooting

### Bot doesn't respond
- Check `BOT_TOKEN` is correct
- Verify bot is running (`docker ps` or check process)
- Check logs for errors

### Database errors
- Ensure `DB_PATH` directory exists and is writable
- Check SQLite version compatibility

### Rate limiting issues
- Adjust `RATE_LIMIT` and `TIME_WINDOW` in `config.py`

## 📄 License

MIT License - Free for educational and commercial use.

## 👥 Contributors

Built for Addis Ababa University campus delivery ecosystem.

---

**Need help?** Contact admins listed in `ADMIN_IDS`.

🚀 **Deliver AAU** - Making campus deliveries smooth and gamified!
