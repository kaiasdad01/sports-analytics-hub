"""NFL data extraction using nflreadpy."""

import logging
from typing import Optional
import polars as pl
import nflreadpy as nfl

logger = logging.getLogger(__name__)


class NFLExtractor:
    """Extract NFL data from nflreadpy."""
    
    # Data types that don't accept seasons parameter
    NO_SEASONS_PARAM = {'teams', 'trades', 'contracts'}
    
    def __init__(self):
        """Initialize the NFL extractor."""
        self.current_season = 2025
    
    def extract(
        self,
        data_type: str,
        seasons: Optional[list[int]] = None,
        **kwargs
    ) -> pl.DataFrame:
        """
        Generic extraction method for any nflreadpy data type.
        
        Args:
            data_type: Type of data to extract (e.g., 'pbp', 'player_stats', 'rosters', 'schedules')
            seasons: List of season years. If None, uses current season. Ignored for data types that don't accept seasons.
            **kwargs: Additional arguments to pass to the nflreadpy function
            
        Returns:
            Polars DataFrame with the requested data
        """
        if data_type not in self.NO_SEASONS_PARAM:
            if seasons is None:
                seasons = [self.current_season]
        
        # Map data types to nflreadpy functions
        data_type_map = {
            'pbp': nfl.load_pbp,
            'play_by_play': nfl.load_pbp,
            'schedules': nfl.load_schedules,
            'rosters': nfl.load_rosters,
            'rosters_weekly': nfl.load_rosters_weekly,
            'player_stats': nfl.load_player_stats,
            'players': nfl.load_players,
            'teams': nfl.load_teams,
            'team_stats': nfl.load_team_stats,
            'depth_charts': nfl.load_depth_charts,
            'officials': nfl.load_officials,
            'injuries': nfl.load_injuries,
            'snap_counts': nfl.load_snap_counts,
            'nextgen_stats': nfl.load_nextgen_stats,
            'participation': nfl.load_participation,
            'combine': nfl.load_combine,
            'draft_picks': nfl.load_draft_picks,
            'contracts': nfl.load_contracts,
            'trades': nfl.load_trades,
            'ftn_charting': nfl.load_ftn_charting,
            'ff_opportunity': nfl.load_ff_opportunity,
            'ff_playerids': nfl.load_ff_playerids,
            'ff_rankings': nfl.load_ff_rankings,
        }
        
        if data_type not in data_type_map:
            raise ValueError(
                f"Unknown data_type '{data_type}'. "
                f"Available types: {list(data_type_map.keys())}"
            )
        
        load_func = data_type_map[data_type]
        
        logger.info(f"Extracting {data_type} for seasons: {seasons}")
        
        try:
            # Only pass seasons if the data type accepts it
            if data_type in self.NO_SEASONS_PARAM:
                df = load_func(**kwargs)
            else:
                df = load_func(seasons=seasons, **kwargs)
            
            logger.info(f"Successfully extracted {len(df)} rows of {data_type}")
            return df
        except Exception as e:
            logger.error(f"Error extracting {data_type}: {e}")
            raise