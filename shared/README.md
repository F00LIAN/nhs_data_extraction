# 📦 Shared Tools

> **What's in here?** These are special tools that BOTH Stage 1 and Stage 2 robots use - like sharing crayons between two kids who are both drawing!

## 🤝 Why "Shared"?

Imagine you have two robots:
- **Robot 1** finds houses
- **Robot 2** gets house details

Instead of giving each robot their own separate toolbox, we put the **common tools** in this shared toolbox that both robots can use!

## 🛠️ Tools in This Toolbox

### 📊 `price_tracker.py` - The Price Detective
**What it does:** Keeps track of how house prices change over time

**Like:** A notebook where you write down how much your favorite toy costs each month to see if it gets cheaper or more expensive.

**Key Jobs:**
- Takes "snapshots" of current house prices
- Compares today's prices with yesterday's prices  
- Saves price history for later analysis
- Creates reports about price trends

**Simple Example:**
```
Day 1: House costs $300,000
Day 2: House costs $305,000  
Day 3: House costs $298,000
→ Price tracker notices: "This house price went down $7,000!"
```

### 📈 `price_history.py` - The Time Machine
**What it does:** Looks at old price data to understand patterns

**Like:** Looking through your old report cards to see if your grades are getting better or worse over time.

**Key Jobs:**
- Analyzes price trends over weeks/months
- Finds houses with the biggest price changes
- Creates charts and graphs about prices
- Helps predict future price movements

**Simple Example:**
```
Looking at House ABC over 6 months:
Jan: $300k → Feb: $305k → Mar: $310k
Trend: "This house is getting more expensive by $5k each month!"
```

## 🔄 How These Tools Work Together

1. **Price Tracker** → Takes daily "photos" of prices
2. **Price History** → Looks at all the "photos" to find patterns

It's like:
- **Price Tracker** = Taking a photo of your height each month
- **Price History** = Looking at all the photos to see how much you've grown

## 🎯 Who Uses These Tools?

- **Stage 1** → Uses price tracker when finding new houses
- **Stage 2** → Uses price tracker when getting detailed house info
- **Reports** → Uses price history to create charts and analysis

## 📝 Important Functions

### From `price_tracker.py`:
- `capture_price_snapshots()` → Takes a "photo" of current prices
- `create_price_snapshot()` → Makes a record of one house's price
- `update_permanent_timelines()` → Saves price data for long-term storage

### From `price_history.py`:
- `analyze_price_trends()` → Looks for patterns in price changes
- `generate_price_reports()` → Creates summaries of price data
- `track_market_changes()` → Watches how the whole market changes

## 🎮 Cool Features

- **Automatic Price Detection** → Finds prices even if websites change
- **Change Alerts** → Notices when prices go up or down significantly  
- **Historical Analysis** → Can look back months or years
- **Market Insights** → Understands neighborhood price trends

## 💾 Database Collections Used

- **pricehistory** → Daily price snapshots
- **price_history_permanent** → Long-term price trends
- **communitydata** → House details with current prices

## 🚨 Important Notes

- These tools run **automatically** when Stage 1 and Stage 2 finish
- They help make sure we never lose track of price changes
- All price data is saved safely in the database
- The tools are smart enough to not duplicate information

---
*These shared tools make sure we never miss important price changes and can understand how the housing market moves over time!* 💰📊
