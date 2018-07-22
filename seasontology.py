import os
import glob
import param
import parambokeh
import numpy as np
import pandas as pd
import hvplot.pandas
import holoviews as hv

from holoviews.streams import Stream
# https://github.com/bokeh/bokeh/pull/8062
from bokeh.themes import built_in_themes

hv.renderer('bokeh').theme = built_in_themes['light_minimal']
hv.extension('bokeh')

DATA = 'data'
CMI = 'CMI'
MRY = 'MRY'

DAY = 'day'
TIME = 'time'
STID = 'stid'
YEAR = 'Year'
SEASON = 'season'
STATION = 'Station'
STATION_ID = 'Station ID'
DAYOFYEAR = 'Day of Year'

WIDTH = 700
HEIGHT = 385

STATES_URL = ('http://www.printabledirect.com/'
              'list-of-all-50-states-abbreviations-chart.htm')

STATION_CSV = os.path.join(DATA, 'station.csv')
STATION_URL_FMT = ('https://mesonet.agron.iastate.edu/sites/'
                   'networks.php?network={0}_ASOS&format=csv&nohtml=on')

WX_PKL_FMT = os.path.join(DATA, '{station}_{dt:%Y}.pkl')
WX_DAILY_URL_FMT = (
    'https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py?'
    'network={iem_network}&station={station}&'
    'year1={dt1.year}&month1={dt1.month}&day1={dt1.day}&'
    'year2={dt2.year}&month2={dt2.month}&day2={dt2.day}'
)

COLORS = ['#e31a1c', '#4daf4a', '#ff7f00', '#377db8']
SEASONS = np.array(['Winter (DJF)',
                    'Spring (MAM)',
                    'Summer (JJA)',
                    'Autumn (SON)'])
SEASON_STARTS = {'2017-12-01': COLORS[3],
                 '2017-03-01': COLORS[1],
                 '2017-06-01': COLORS[2],
                 '2017-09-01': COLORS[0]}

VAR_RANGE = {
    'Max Temp F': (-10, 120),
    'Min Temp F': (-15, 115),
    'Max Dewpoint F': (-10, 100),
    'Min Dewpoint F': (-10, 115),
    'Precip In': (0, 0.5),
    'Avg Wind Speed Kts': (0, 35),
    'Avg Wind Drct': (-10, 380),
    'Min Rh': (0, 120),
    'Avg Rh': (0, 120),
    'Max Rh': (0, 120),
    'Climo High F': (-10, 120),
    'Climo Low F': (-40, 80),
    'Climo Precip In': (0, 0.5),
    'Min Feel': (-10, 120),
    'Avg Feel': (-15, 115),
    'Max Feel': (-10, 120)
}


def _states_abbr():
    """Get the fifty states two letter acroynm i.e. IL
    """
    states_df = pd.read_html(STATES_URL)[0].melt()
    states_abbr = states_df[states_df['value'].str.len() == 2].value
    return states_abbr


def _station_df():
    """Get all US stations' metadata as a dataframe
    """
    if os.path.exists(STATION_CSV):
        station_df = pd.read_csv(STATION_CSV, index_col=STID)
    else:
        states_abbr = _states_abbr()
        station_df = pd.concat(pd.read_csv(STATION_URL_FMT.format(state_abbr),
                                           index_col=STID)
                               for state_abbr in states_abbr)
        station_df.to_csv(STATION_CSV)
    return station_df


def _season_vlines():
    season_vlines = hv.Overlay(
        [hv.VLine(pd.to_datetime(season_start).dayofyear)
         .options(line_width=1.5, alpha=0.9, color=color, line_dash='dotted')
         for season_start, color in SEASON_STARTS.items()]
    )
    return season_vlines


def _standardize_station(station):
    if len(station) > 3:
        station = station.lstrip('K')

    return station.upper()


def cache_daily_wx_df(station):
    """Get daily weather data as a dataframe
    """
    if station in cached_wx_dfs:
        wx_df = cached_wx_dfs[station]
    else:
        end_dt = pd.datetime.now()
        wx_pkl = WX_PKL_FMT.format(station=station,
                                   dt=end_dt)
        if os.path.exists(wx_pkl):
            wx_df = pd.read_pickle(wx_pkl)
        else:
            station_df = _station_df().loc[station]
            iem_network = station_df['iem_network']
            ini_dt = pd.to_datetime(station_df['begints'])
            wx_daily_url_fmtd = WX_DAILY_URL_FMT.format(
                station=station, iem_network=iem_network,
                dt1=ini_dt, dt2=end_dt)
            wx_df = pd.read_csv(wx_daily_url_fmtd,
                                index_col=DAY,
                                parse_dates=True,
                                na_values=[-99.0, 'None'])
            wx_df.columns = (wx_df.columns
                             .str.replace('_', ' ')
                             .str.title())
            wx_df.index.name = TIME
            wx_df = wx_df.assign(
                **{SEASON: SEASONS[(wx_df.index.month // 3) % 4],
                   DAYOFYEAR: wx_df.index.dayofyear,
                   YEAR: wx_df.index.year})
            # make the colors consistent hack
            wx_df = wx_df.sort_values(SEASON)
            wx_df.to_pickle(wx_pkl)
        cached_wx_dfs[station] = wx_df
    return wx_df


def _plot_kde(df_hv, var):
    return (df_hv.kde(var, groupby=[SEASON, STATION],
                      invert=True, color=hv.Cycle(COLORS))
                .options(show_grid=True, width=WIDTH, height=HEIGHT)
                .redim.range(**{var: VAR_RANGE[var]})
                .overlay(SEASON))

def _plot_ts(df_hv, var):
    return (df_hv.points(DAYOFYEAR, var, groupby=STATION,
                         hover_cols=[YEAR])
                .options(width=WIDTH, height=HEIGHT,
                         tools=['hover'], show_grid=True,
                         cmap='inferno', color_index=YEAR,
                         size=4, alpha=0.5, colorbar=True,
                         colorbar_opts=dict(label_standoff=12, title=YEAR))
                .redim.range(**{var: VAR_RANGE[var]})) * season_vlines

def _add_label(yloc, station):
    return hv.Text(5, yloc, station).options(
        color='#5B5B5B', text_align='left', text_baseline='top',
        text_font='Helvetica', text_font_style='bold',
        text_font_size='2.00em', text_alpha=0.15)


def plot_stations(station_one, station_two, var):
    """High level function to create two plots
    """
    station_one = _standardize_station(station_one)
    station_two = _standardize_station(station_two)

    df1 = cache_daily_wx_df(station_one)
    df2 = cache_daily_wx_df(station_two)
    df = pd.concat([df1, df2])

    yloc = VAR_RANGE[var][1] - 3
    text1 = _add_label(yloc, station_one)
    text2 = _add_label(yloc, station_two)

    df_hv = df.hvplot
    layout = (
        _plot_ts(df_hv, var).select(**{STATION: station_one}) * text1 +
        _plot_kde(df_hv, var).select(**{STATION: station_one}) +
        _plot_ts(df_hv, var).select(**{STATION: station_two}) * text2 +
        _plot_kde(df_hv, var).select(**{STATION: station_two})
    ).cols(2)

    return layout


class Seasontology(Stream):
    """Highest level function to run the interactivity
    """

    station_one = param.String(default=CMI, doc=STATION_ID)
    station_two = param.String(default=MRY, doc=STATION_ID)

    variable = param.ObjectSelector(
        default=list(VAR_RANGE.keys())[0],
        objects=list(VAR_RANGE.keys()),
    )
    data = param.Parameter(default="mesonet.agron.iastate.edu",
                           constant=True, precedence=0.)

    output = parambokeh.view.Plot()

    def view(self, *args, **kwargs):
        return plot_stations(self.station_one,
                             self.station_two,
                             self.variable)

    def event(self, **kwargs):
        # clear canvas and replace
        if not self.output or any(k in kwargs for k in ['station_one',
                                                        'station_two']):
            self.output = hv.DynamicMap(self.view, streams=[self])
        else: # update canvas with new variable
            super(Seasontology, self).event(**kwargs)


# initialize
cached_wx_dfs = {}

this_year = pd.datetime.now().year
for pkl in glob.iglob(os.path.join(DATA, '*.pkl')):
    # clean up cached files if stale
    if str(this_year) not in pkl:
        os.remove(pkl)

# pre-generate delimiters so it doesn't have to be generated everytime
season_vlines = _season_vlines()
selector = Seasontology(name='Seasontology')
parambokeh.Widgets(selector,
                   width=250,
                   on_init=True,
                   mode='server',
                   view_position='right',
                   callback=selector.event)
