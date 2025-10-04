## Travel Pics Game stuff

Some scripts for dealing with the [Travel Pics Game](https://tinyurl.com/tpgrulesfaq) in Chicago Geographer's Discord server. Basically all of this is a command line interface for stuff in [travelpygame](https://github.com/Miss-Inputs/travelpygame/tree/main), so you'll want to set up a virtual environment with that installed (via git, etc).

To get started you'll probably need to set some environment variables to decide where to save some files, see settings.py for which ones, though if I rewrite this then you won't need to do that anymore.

### Terminology
The phrase "TPG data" or "TPG data file" is something I haven't invented the best terminology for yet, it is a .json file containing a list of rounds and submissions for each round, either from main TPG or a spinoff. Sometimes I end up saying something awkward like "rounds and submissions" which is a bit of a mouthful.

The phrase "point set" is also something I haven't figured out the best wording for, it refers to a file containing a list of locations that you have pics for (with optional name/description), such as a .csv exported from [scottytremaine's voronoi generator](https://tpg.scottytremaine.uk/), or can be an .ods or .xlsx spreadsheet, or a .geojson/.gpkg/etc file containing point geometries, or various other formats that pandas DataFrames can be loaded from. "Target points" or "target point set" refers to these same formats but instead of locations that you have pics for/have been to, it represents locations that you want the distance to, e.g. TPG rounds.

The phrase "TPG area" or "defined area" refers to the part of the world that rounds in a regional TPG (AusTPG, EuroTPG, Japan TPG, etc) can be drawn from.

### For TPG players
Things to help you actually play the game.
- Get all midpoints: Intended for Team TPG, finds every possible midpoint between all of your points and a teammate's points.
- Reverse geocode all points: Given a point set with no or incomplete name/description, uses a Nominatim mirror to find the address for each point, and optionally saves the file to .csv or .geojson or whichever else. (The reverse geocoding is not in travelpygame at this point in time.)

### For TPG players (stats)
Things to get stats for you/your point set, which may be useful, or may be just interesting.
- Get best pic for points: Given a target point set, finds your closest point in your point set for each one.
- Point set stats: Outputs some info about your point set, such as average/central location, or furthest possible point on the earth.

### For TPG travellers
Things to help figure out where to actually go in real life so you can have a better score in Travel Pics Game.
- Closest wins: With your username (and optionally a TPG data file), prints the person one place ahead of you for each round, how much ahead of you they were and in which direction, and optionally what point is that distance in that direction.
- Evaluate new points: Given a point set, and also another point set for locations you haven't been to yet but are considering, this figures out which points in the new point set would be the most useful in various ways, optionally using a set of targets (which can be yet another point set, or a TPG data file to use the rounds from it) in which case it will find which new points get you closer to a target than your old points.

### For TPG spinoff runners
- Convert submission tracker: Converts kml/kmz file(s) from a submission tracker (or several, if a season needs multiple trackers) hosted on Google My Maps to a TPG data file, for easier use with everything else.
- Distribution stats for TPG area: Displays the total area in metres and percentage that each region (.geojson/.gpkg) occupies in a regional TPG area, for example, the regions file can be official subdivision boundaries from a government agency, and this will show which subdivisions are the biggest in a TPG area.
- Generate random location: Given a .geojson/.gpkg/etc file containing a TPG area, picks out a random point somewhere in that area, or optionally multiple points, and optionally prints some stats.
- Round stats: Outputs some stats (e.g. average/central submission, average distance) for each round in a TPG data file that has scores.
- Score submissions: Takes TPG data files or exported submission trackers, calculates distances and scores according to options, saves as a TPG data file containing scores, outputs leaderboards, and prints submission reminders.
- Stats for TPG area: Prints some stats for a TPG area, such as counts of category columns (can be autodetected from frequencies or manually specified, as most geographical file formats don't store anything that indicates a column is a pandas "category" type), or centroid, representative point, pole of inaccessibility, etc.

### Other stats
- Custom travel map: Plots a map of regions specified from an arbitrary geo file (.geojson/.gpkg/etc) with how many people have been to each one, according to TPG data.
- Get main TPG data: Gets main TPG data and optionally scores it (does so by default), for use with everything else.
- Per-user submission stats: Gets some stats for each player in TPG data, such as their furthest possible point and how far away that is. NOTE: This currently needs reworking and also just saves files into /tmp instead of anywhere sensible or configurable.
- Plot user submissions: Plots rounds and submissions and arrows from the submission to the round, given TPG data and a player name. NOTE: This currently needs rewriting and also hardcodes some stuff.

### Unsorted
- Get elevation from DEM: Given your point set, and a .tiff file or whatever representing a DEM (digital elevation model), finds the elevation of your points (where the DEM has that data). Elevation is not used in TPG except in some hypothetical spinoffs, but maybe you might like to add that as a column for funsies.

### Unsorted and may be dismantled/deleted
- Concave hull: Was just here for messing around, but could be merged into point set stats if it's useful
- Most unique submissions: Submissions that are the furthest apart from any other submissions. Needs refactoring to use the new format for TPG data, etc.
- Submission stats for user: Prints some stats from main TPG data, but needs to use the new format for that.
- Submission tracker to GeoJSON: Was probably just there for testing but should be using a TPG data file instead.
- Theoretical best for each user: Re-runs each TPG round with every user's known pics, finding what everyone would have picked for each round if everyone played every round in history and always had every pic that they are known to have. Needs rewrite and currently won't work.

### One-off things which are just around if one desires to run them again
- TPG wrapped.py: Used at the end of season 2 to generate nicely formatted stats for every player, which everyone liked.
- More TPG stats.py: Some other stats requested by CG for the season 2 award ceremony.