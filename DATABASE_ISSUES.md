# Database Design Issues: Denormalized Team Storage and Missing Constraints

## Overview

The database design has several critical issues that impact data integrity, query performance, and maintainability. The most significant problem is the denormalized team storage using space-delimited strings.

## Critical Issues

### 1. Denormalized Team Storage (Priority: Critical)

**Current Implementation:**
- Teams stored as space-delimited strings in `games.team1` and `games.team2`
- Example: `"123456789 987654321 456789123 789123456 321654987"`
- Captain inferred from position (first ID in string)
- Locations: `database.py:76-77`

**Problems:**
- ❌ No referential integrity - can store invalid Discord IDs
- ❌ Cannot efficiently query "all games for player X"
- ❌ String parsing scattered across codebase (10+ locations)
- ❌ No validation of data format
- ❌ Maintenance burden - error-prone string manipulation

**Impact:**
- `shazbuckbot.py:1250-1251, 1293-1294, 1647-1648, 1708-1709` - String parsing
- `utils/trueskill_recalc.py:61, 63, 68, 80, 88` - String parsing

### 2. Missing Foreign Key Constraints (Priority: High)

**Missing FKs:**
- `transfers.sender` → `users.id`
- `transfers.receiver` → `users.id`
- `wagers.user_id` → `users.id`
- `wagers.game_id` → `games.id`
- `trueskills.game_id` → `games.id`

**Problems:**
- Can create orphaned records
- No cascading deletes
- Data integrity issues

### 3. Missing Indexes (Priority: High)

**No indexes on:**
- `users.discord_id` (frequently queried)
- `games.status` (queried in `database.py:350`)
- `wagers.game_id` (queried in `database.py:474`)
- `wagers(user_id, game_id)` composite
- `trueskills.discord_id` and `game_id`

**Impact:** Slow queries as data grows, full table scans

### 4. TrueSkills Table Not in Main Schema (Priority: Medium)

**Problem:**
- Created separately in `utils/trueskill_recalc.py:33-42`
- Not in `database.py:new_database()`
- New installations fail when querying trueskills

## Recommendations

### Phase 1: Non-Breaking Changes (Immediate)

**Add Indexes:**
```sql
CREATE UNIQUE INDEX idx_users_discord_id ON users(discord_id);
CREATE INDEX idx_games_status ON games(status);
CREATE INDEX idx_games_queue_status ON games(queue, status);
CREATE INDEX idx_wagers_game_id ON wagers(game_id);
CREATE INDEX idx_wagers_user_game ON wagers(user_id, game_id);
CREATE INDEX idx_wagers_game_result ON wagers(game_id, result);
CREATE INDEX idx_transfers_sender ON transfers(sender);
CREATE INDEX idx_transfers_receiver ON transfers(receiver);
CREATE INDEX idx_trueskills_discord_id ON trueskills(discord_id);
CREATE INDEX idx_trueskills_game_id ON trueskills(game_id);
```

**Move TrueSkills to Main Schema:**
- Add trueskills table creation to `database.py:new_database()`

### Phase 2: Add Foreign Keys (Short Term)

```sql
-- For new tables, add FKs to schema
FOREIGN KEY (sender) REFERENCES users(id)
FOREIGN KEY (receiver) REFERENCES users(id)
FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
```

Note: SQLite doesn't support adding FKs to existing tables. Will need to recreate tables.

### Phase 3: Normalize Team Storage (Long Term)

**Create new table:**
```sql
CREATE TABLE game_participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,  -- References users.id
    team INTEGER NOT NULL,     -- 1 or 2
    is_captain INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(game_id, user_id)
);

CREATE INDEX idx_participants_game ON game_participants(game_id);
CREATE INDEX idx_participants_user ON game_participants(user_id);
CREATE INDEX idx_participants_team ON game_participants(game_id, team);
```

**Benefits:**
- ✅ Query "all games for player X" efficiently
- ✅ Referential integrity enforced
- ✅ Explicit captain designation
- ✅ Can add player-specific metadata
- ✅ Eliminate string parsing throughout codebase

**Migration Strategy:**
1. Create `game_participants` table
2. Migrate data from `team1`/`team2` strings
3. Update code to use new table
4. Drop old columns after verification

### Additional Improvements

**Add Check Constraints:**
```sql
CHECK (team IN (1, 2))
CHECK (amount > 0)
```

**Field Whitelisting:**
- `database.py:144, 160, 399` use f-strings with user input
- Add whitelisting for field names to prevent injection

## Effort Estimate

- **Phase 1 (Indexes):** 2-4 hours (low risk)
- **Phase 2 (Foreign Keys):** 1-2 days (requires table recreation)
- **Phase 3 (Normalization):** 2-3 days (significant refactoring)

## Priority Recommendation

1. **Immediate:** Add all indexes (performance boost, zero risk)
2. **Short term:** Move trueskills to main schema, add check constraints
3. **Medium term:** Add foreign key constraints
4. **Long term:** Normalize team storage (highest value, requires most work)

The string-based team storage is the biggest technical debt and will become increasingly problematic as the codebase grows.
