nflreadpy homepage: https://nflreadpy.nflverse.com/
Load Functions: https://nflreadpy.nflverse.com/api/load_functions/
Configuration: https://nflreadpy.nflverse.com/api/configuration/
Cache Management: https://nflreadpy.nflverse.com/api/cache/
Utilities: https://nflreadpy.nflverse.com/api/utils/
Changelog: https://nflreadpy.nflverse.com/CHANGELOG/
Data Dictionaries: https://nflreadr.nflverse.com/articles/index.html
NFLverse Github: https://github.com/nflverse
Automation Page - this shows where nflreadpy downloads data from, and when: https://nflreadr.nflverse.com/articles/nflverse_data_schedule.html

# NFLreadpy Package Documentation

## Overview

NFLreadpy is a Python package that provides easy access to NFL data from the NFLverse ecosystem. It serves as the primary data acquisition tool for NFL statistics, game data, player information, and more. This documentation serves as the comprehensive reference for using nflreadpy in our NFL analytics project.

## Table of Contents

1. [Installation & Setup](#installation--setup)
2. [Core Concepts](#core-concepts)
3. [Load Functions](#load-functions)
4. [Configuration](#configuration)
5. [Cache Management](#cache-management)
6. [Utilities](#utilities)
7. [Data Dictionaries](#data-dictionaries)
8. [Data Schedule & Automation](#data-schedule--automation)
9. [Common Use Cases](#common-use-cases)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

## Installation & Setup

### Prerequisites
- Python 3.8+
- Internet connection for data downloads
- Sufficient disk space for caching (recommended: 1GB+)

### Installation
```bash
pip install nflreadpy
```

### Basic Import
```python
import nflreadpy as nfl
```

## Core Concepts

### Data Sources
NFLreadpy aggregates data from multiple sources:
- **NFL.com**: Official NFL statistics and game data
- **Pro Football Reference**: Historical data and advanced metrics
- **ESPN**: Additional player and team statistics
- **Sports Reference**: Comprehensive historical records

### Data Types
The package provides access to various data categories:
- **Game Data**: Play-by-play, game results, schedules
- **Player Data**: Rosters, statistics, career records
- **Team Data**: Standings, team statistics, coaching staff
- **Advanced Metrics**: EPA, CPOE, DVOA, and other advanced analytics

## Load Functions

### Primary Data Loading Functions

#### Game Data
```python
# Load play-by-play data
pbp = nfl.load_pbp(2023)

# Load schedules
schedules = nfl.load_schedules(2023)
```

#### Player Data
```python
# Load player rosters
rosters = nfl.load_rosters(2023)

# Load weekly rosters
rosters_weekly = nfl.load_rosters_weekly(2023)

# Load player statistics
player_stats = nfl.load_player_stats(2023)

# Load player information
players = nfl.load_players(2023)
```

#### Team Data
```python
# Load team information
teams = nfl.load_teams()

# Load team statistics
team_stats = nfl.load_team_stats(2023)
```

#### Additional Data Types
```python
# Load depth charts
depth_charts = nfl.load_depth_charts(2023)

# Load officials data
officials = nfl.load_officials(2023)

# Load injury reports
injuries = nfl.load_injuries(2023)

# Load snap counts
snap_counts = nfl.load_snap_counts(2023)

# Load Next Gen Stats
nextgen_stats = nfl.load_nextgen_stats(2023)

# Load participation data
participation = nfl.load_participation(2023)

# Load combine results
combine = nfl.load_combine(2023)

# Load draft picks
draft_picks = nfl.load_draft_picks(2023)

# Load contracts
contracts = nfl.load_contracts(2023)

# Load trades
trades = nfl.load_trades(2023)

# Load FTN charting data
ftn_charting = nfl.load_ftn_charting(2023)

# Load fantasy football opportunity data
ff_opportunity = nfl.load_ff_opportunity(2023)

# Load fantasy football player IDs
ff_playerids = nfl.load_ff_playerids()

# Load fantasy football rankings
ff_rankings = nfl.load_ff_rankings(2023)
```

### Advanced Data Loading

#### Play-by-Play Data
```python
# Load specific weeks
pbp_week1 = nfl.load_pbp(2023, week=1)

# Load specific teams
pbp_chiefs = nfl.load_pbp(2023, team='KC')

# Load specific games
pbp_game = nfl.load_pbp(2023, game_id='2023_01_KC_BUF')
```

#### Historical Data
```python
# Load multiple seasons
pbp_historical = nfl.load_pbp([2020, 2021, 2022, 2023])

# Load specific date range
pbp_range = nfl.load_pbp(2023, start_date='2023-09-01', end_date='2023-12-31')
```

## Configuration

### Environment Variables
```python
# Set cache directory
import os
os.environ['NFLREADPY_CACHE_DIR'] = '/path/to/cache'

# Set data source
os.environ['NFLREADPY_DATA_SOURCE'] = 'nfl.com'
```

### Configuration Options
```python
# Configure data source
nfl.configure(data_source='nfl.com')

# Configure cache settings
nfl.configure(cache_dir='/custom/cache/path')

# Configure download settings
nfl.configure(parallel_downloads=4)
```

## Cache Management

### Cache Overview
NFLreadpy uses intelligent caching to:
- Reduce API calls and improve performance
- Store data locally for offline access
- Manage disk space efficiently
- Handle data updates automatically

### Cache Operations
```python
# Check cache status
cache_info = nfl.cache_info()

# Clear cache
nfl.clear_cache()

# Clear specific data
nfl.clear_cache(data_type='pbp', season=2023)

# Set cache size limit
nfl.configure(max_cache_size='2GB')
```

### Cache Directory Structure
```
cache/
├── pbp/
│   ├── 2023/
│   │   ├── week_1.csv
│   │   └── week_2.csv
│   └── 2022/
├── games/
│   └── 2023/
└── rosters/
    └── 2023/
```

## Utilities

### Data Validation
```python
# Validate data integrity
nfl.validate_data(pbp_data)

# Check data completeness
completeness = nfl.check_completeness(pbp_data)
```

### Data Transformation
```python
# Convert data types
pbp_typed = nfl.convert_types(pbp_data)

# Add derived columns
pbp_enhanced = nfl.add_derived_columns(pbp_data)
```

### Data Export
```python
# Export to different formats
nfl.export_data(pbp_data, format='parquet', path='data/pbp_2023.parquet')

# Export with compression
nfl.export_data(pbp_data, format='csv', compression='gzip')
```

## Data Dictionaries

### Play-by-Play Data Dictionary
Key columns in play-by-play data:
- `game_id`: Unique game identifier
- `play_id`: Unique play identifier
- `posteam`: Team with possession
- `defteam`: Defending team
- `down`: Down number
- `ydstogo`: Yards to go for first down
- `yardline_100`: Yard line (0-100 scale)
- `play_type`: Type of play (pass, run, etc.)
- `epa`: Expected Points Added
- `wpa`: Win Probability Added

### Game Data Dictionary
Key columns in game data:
- `game_id`: Unique game identifier
- `season`: Season year
- `week`: Week number
- `home_team`: Home team abbreviation
- `away_team`: Away team abbreviation
- `home_score`: Home team final score
- `away_score`: Away team final score
- `result`: Game result for home team

### Player Data Dictionary
Key columns in player data:
- `player_id`: Unique player identifier
- `player_name`: Player's full name
- `position`: Player position
- `team`: Team abbreviation
- `season`: Season year
- `week`: Week number
- `passing_yards`: Passing yards
- `rushing_yards`: Rushing yards
- `receiving_yards`: Receiving yards

## Data Schedule & Automation

### Data Update Schedule
NFLreadpy follows the NFL data release schedule:
- **Game Data**: Updated within 24 hours of game completion
- **Play-by-Play**: Updated within 2-4 hours of game completion
- **Player Stats**: Updated daily during the season
- **Rosters**: Updated as changes occur

### Automation Features
```python
# Enable automatic updates
nfl.configure(auto_update=True)

# Set update frequency
nfl.configure(update_frequency='daily')

# Check for updates
updates_available = nfl.check_updates()
```

## Common Use Cases

### 1. Loading Recent Game Data
```python
# Load current season data
current_season = 2023
games = nfl.load_games(current_season)
pbp = nfl.load_pbp(current_season)

# Filter for recent games
recent_games = games[games['week'] >= 15]
```

### 2. Player Performance Analysis
```python
# Load player statistics
player_stats = nfl.load_player_stats(2023)

# Filter for specific position
qb_stats = player_stats[player_stats['position'] == 'QB']

# Calculate performance metrics
qb_stats['passing_efficiency'] = qb_stats['passing_yards'] / qb_stats['attempts']
```

### 3. Team Comparison
```python
# Load team data
teams = nfl.load_teams()
team_stats = nfl.load_team_stats(2023)

# Compare teams
team_comparison = team_stats.groupby('team').agg({
    'total_yards': 'sum',
    'total_points': 'sum',
    'wins': 'sum'
})
```

### 4. Historical Analysis
```python
# Load multiple seasons
historical_data = nfl.load_pbp([2020, 2021, 2022, 2023])

# Analyze trends over time
yearly_stats = historical_data.groupby('season').agg({
    'epa': 'mean',
    'wpa': 'mean'
})
```

## Best Practices

### 1. Data Loading Strategy
```python
# Load data incrementally
def load_season_data(season):
    """Load data for a specific season with error handling."""
    try:
        games = nfl.load_games(season)
        pbp = nfl.load_pbp(season)
        return games, pbp
    except Exception as e:
        print(f"Error loading data for {season}: {e}")
        return None, None
```

### 2. Memory Management
```python
# Load data in chunks for large datasets
def load_large_dataset(seasons):
    """Load large datasets efficiently."""
    for season in seasons:
        data = nfl.load_pbp(season)
        # Process data immediately
        process_data(data)
        # Clear from memory
        del data
```

### 3. Caching Strategy
```python
# Configure cache for your use case
nfl.configure(
    cache_dir='/path/to/cache',
    max_cache_size='5GB',
    auto_cleanup=True
)
```

### 4. Error Handling
```python
# Robust data loading with error handling
def safe_load_data(data_type, **kwargs):
    """Safely load data with error handling."""
    try:
        if data_type == 'pbp':
            return nfl.load_pbp(**kwargs)
        elif data_type == 'games':
            return nfl.load_games(**kwargs)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    except Exception as e:
        print(f"Error loading {data_type}: {e}")
        return None
```

## Troubleshooting

### Common Issues

#### 1. Network Connectivity
```python
# Check network connectivity
import requests
try:
    response = requests.get('https://nflreadpy.nflverse.com/', timeout=5)
    print("Network connection successful")
except:
    print("Network connection failed")
```

#### 2. Cache Issues
```python
# Clear cache if experiencing issues
nfl.clear_cache()
nfl.configure(cache_dir='/new/cache/path')
```

#### 3. Data Validation
```python
# Validate loaded data
def validate_data(data):
    """Validate data integrity."""
    if data is None:
        print("No data loaded")
        return False
    
    if len(data) == 0:
        print("Empty dataset")
        return False
    
    print(f"Data loaded successfully: {len(data)} rows")
    return True
```

#### 4. Performance Issues
```python
# Monitor performance
import time

start_time = time.time()
data = nfl.load_pbp(2023)
end_time = time.time()

print(f"Data loading took {end_time - start_time:.2f} seconds")
```

### Error Messages and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `ConnectionError` | Network issues | Check internet connection, retry |
| `CacheError` | Cache corruption | Clear cache, reconfigure |
| `DataError` | Invalid data format | Update nflreadpy, check data source |
| `MemoryError` | Insufficient memory | Load data in chunks, increase RAM |

## Additional Resources

- **Official Documentation**: https://nflreadpy.nflverse.com/
- **API Reference**: https://nflreadpy.nflverse.com/api/
- **GitHub Repository**: https://github.com/nflverse
- **Data Dictionaries**: https://nflreadr.nflverse.com/articles/index.html
- **Automation Schedule**: https://nflreadr.nflverse.com/articles/nflverse_data_schedule.html

## Version Information

This documentation is based on nflreadpy version 1.0+. For the latest version information and changelog, visit: https://nflreadpy.nflverse.com/CHANGELOG/

---

*This documentation serves as the primary reference for NFL data acquisition in our analytics project. For specific implementation details, refer to the individual function documentation and examples provided above.*
