The .csv files available for download are comprised of seven master .csv files

allplayers.csv - contains basic information about all players divided by team-season
gameinfo.csv - contains game-level information such as teams, attendance, umpires, etc.
teamstats.csv - contains team-level statistics - line scores, lineups, and team statistics (batting, pitching, fielding)
batting.csv - batting statistics by player by game
pitching.csv - pitching statistics by player by game
fielding.csv - fielding statistics by player by position by game
plays.csv - parsed play-by-play data for all games for which Retrosheet has play-by-play data (including deduced event files)





DATA DEFINITION

It is hoped that the column headings for the seven basic .csv files available for download by Retrosheet will be self-explanatory. But just in case not, here are the columns.



allplayers.csv
--------------
id               RetroID
last             Last name
first            First name
bat              How a player bats: B (both), L, R
throw            How a player throws - unknown batting or throwing will be indicated by "?"
team             Retrosheet's TeamID - each player will have a separate line for each team for which the team appeared
g		 Games played
g_p		 Games played as a pitcher
g_sp		 Games played as a starting pitcher
g_rp		 Games played as a relief pitcher
g_c		 Games played as a catcher
g_1b		 Games played as a first baseman
g_2b		 Games played as a second baseman
g_3b		 Games played as a third baseman
g_ss		 Games played as a shortstop
g_lf		 Games played as a left fielder
g_cf		 Games played as a center fielder
g_rf		 Games played as a right fielder
g_of		 Games played as a outfielder
g_dh		 Games played as a designated hitter
g_ph		 Games played as a pinch hitter
g_pr		 Games played as a pinch runner
first_g		 First game played by player in season for team for which Retrosheet has game information
last_g		 Last game played by player in season for team for which Retrosheet has game information - first_g, last_g include all gametypes
season		 Season - players are listed separate for each team-season in which they played

gameinfo.csv
------------
gid              GameID
visteam          visiting team
hometeam         home team
site             site of game
date             date of game
number           game number = 0 if a single game; 1, 2, or (in rare cases) 3, otherwise
starttime        starting time of game, if known
daynight         day or night game
innings          scheduled innings - blank indicates game was scheduled for 9 innings
tiebreaker       did game use a tiebreaker (e.g., runner on 2b to start extra innings)?
usedh            did game use a DH?
htbf             did the home team bat first? - blanks for any of 'tiebreaker', 'usedh', or 'htbf' indicate an answer of 'no'
timeofgame       length of game, in minutes
attendance       attendance
fieldcond        field conditions, if known
precip           precipitation, if known
sky              sky conditions, if known
temp             temperature at game time
winddir          wind direction
windspeed        wind speed
oscorer          official scorer, if known
forfeit          was the game forfeited - blank indicates no forfeit
suspend		 was the game suspended - blank indicates no; if game was suspended, the date on which the game was completed is indicated here
umphome          home-plate umpire, if known
ump1b            1st-base umpire
ump2b            2nd-base umpire
ump3b            3rd-base umpire
umplf            left field umpire
umprf            right field umpire
wp               winning pitcher
lp               losing pitcher
save             pitcher who earned save, if any
gametype         type of game (e.g., regular-season, exhibition, etc.)
                 - gametype 'playoff' indicates tiebreaker games (e.g., the "Bobby Thomson" and "Bucky Dent" games) which have historically
                   been treated as regular-season games by MLB
vruns            visiting team runs scored
hruns            home team runs scored
wteam            winning team
lteam            losing team
line             do we know the line score of the game - blank indicates 'no'
batteries        do we know the teams' batteries; p = pitchers, both = pitchers & catchers; blank indicates batteries not known
lineups          do we know the teams' lineups; blank indicates 'no'
box              do we have a box score; blank indicates 'no'
pbp              do we have a play-by-play account; d = deduced account, y = play-by-play account from newspaper or scorecard; blank indicates 'no'
season           season in which game took place

teamstats.csv
-------------
gid              Game ID
team             Team ID
inn1             runs scored by team in 1st inning
inn2             runs scored by team in 2nd inning
inn3             runs scored by team in 3rd inning
inn4             runs scored by team in 4th inning
inn5             runs scored by team in 5th inning
inn6             runs scored by team in 6th inning
inn7             runs scored by team in 7th inning
inn8             runs scored by team in 8th inning
inn9             runs scored by team in 9th inning
inn10 - inn28    runs scored by team in inning # {10 - 28)
lob              runners left on base by team
mgr              in-game manager for team; this field has not been populated yet for Negro League games
stattype         'value', 'official', 'lower', or 'upper': 
                 'upper' and 'lower' bounds are shown for some stats to highlight data uncertainty
                 'official' identifies discrepancies between Retrosheet accounts and official statistics
b_pa             plate appearances by team's batters
b_ab             at bats by team's batters
b_r              runs scored
b_h              hits
b_d              doubles
b_t              triples
b_hr             home runs
b_rbi            runs batted in
b_sh             sacrifice hits
b_sf             sacrifice flies
b_hbp            times hit by pitch for team's batters
b_w              batter walks by team
b_iw             intentional walks
b_k              batter strikeouts by team
b_sb             stolen bases
b_cs             times caught stealing
b_gdp            times grounded in double play by team's batters
b_xi             times reached base on catcher's interference
b_roe            times reached base via error
p_ipouts         outs recorded by team pitchers (IP * 3)
p_noout          batters faced by team's pitchers in any innings in which no outs were recorded
p_bfp            batters faced by team's pitchers
p_h              hits allowed
p_d              doubles allowed
p_t              triples allowed
p_hr             home runs allowed
p_r              runs allowed
p_er             earned runs allowed
p_w              walks allowed
p_iw             intentional walks allowed
p_k              strikeouts by team's pitchers
p_hbp            hit by pitches allowed
p_wp             wild pitches allowed
p_bk             balks allowed
p_sh             sacrifice hits allowed
p_sf             sacrifice flies allowed
p_sb             stolen bases allowed
p_cs             caught stealing allowed
p_pb             passed balls allowed
d_po             team putouts
d_a              fielding assists by team
d_e              errors committed by team
d_dp             double plays turned by team
d_tp             triple plays turned by team
d_pb             passed balls allowed by team
d_wp             wild pitches allowed by team
d_sb             stolen bases allowed by team
d_cs             caught stealing allowed by team
start_l1         starter at lineup position 1
start_l2         starter at lineup position 2
start_l3         starter at lineup position 3
start_l4         starter at lineup position 4
start_l5         starter at lineup position 5
start_l6         starter at lineup position 6
start_l7         starter at lineup position 7
start_l8         starter at lineup position 8
start_l9         starter at lineup position 9
start_f1         starter at fielding position 1 (i.e., pitcher)
start_f2         starter at fielding position 2 (catcher)
start_f3         starter at fielding position 3 (1B)
start_f4         starter at fielding position 4 (2B)
start_f5         starter at fielding position 5 (3B)
start_f6         starter at fielding position 6 (SS)
start_f7         starter at fielding position 7 (LF)
start_f8         starter at fielding position 8 (CF)
start_f9         starter at fielding position 9 (RF)
start_f10        starter at fielding position 10 (DH)
date             date of game
number           game number (0 if single game; 1, 2, etc. if multiple games on same date)
site             location of game
vishome		 'v' if team was visiting team; 'h' if team was home team
opp              team's opponent in game
win              equal to one if team won the game
loss		 equal to one if team lost the game
tie		 equal to one if game ended in a tie
gametype         type of game (e.g., regular-season, exhibition, etc.)
box              do we have a box score; blank indicates 'no'
pbp              do we have a play-by-play account; d = deduced account, y = play-by-play account from newspaper or scorecard; blank indicates 'no'

batting.csv
-----------
gid              Game ID
id               Player ID
team             Team ID
b_lp             player's lineup position (if known)
b_seq            order in which the player appears
                 (1 = starter, 2 = 2nd player at given lineup slot, etc.)
stattype         'value', 'official', 'lower', or 'upper': 
                 'upper' and 'lower' bounds are shown for some stats to highlight data uncertainty
                 'official' identifies discrepancies between Retrosheet accounts and official statistics
b_pa             plate appearances
b_ab             at bats
b_r              runs scored
b_h              hits
b_d              doubles
b_t              triples
b_hr             home runs
b_rbi            runs batted in
b_sh             sacrifice hits
b_sf             sacrifice flies
b_hbp            times hit by pitch
b_w              walks
b_iw             intentional walks
b_k              strikeouts
b_sb             stolen bases
b_cs             times caught stealing
b_gdp            times grounded in double play by team's batters
b_xi             times reached base on catcher's interference
b_roe            times reached base via error
dh               did the player DH in the game
ph               did the player pinch hit
pr               did the player pinch run
date             date of game
number           game number (0 if single game; 1, 2, etc. if multiple games on same date)
site             location of game
vishome		 'v' if team was visiting team; 'h' if team was home team
opp              team's opponent in game
win              equal to one if team won the game
loss		 equal to one if team lost the game
tie		 equal to one if game ended in a tie
gametype         type of game (e.g., regular-season, exhibition, etc.)
box              do we have a box score; blank indicates 'no'
pbp              do we have a play-by-play account; d = deduced account, y = play-by-play account from newspaper or scorecard; blank indicates 'no'

pitching.csv
------------
gid              Game ID
id               Player ID
team             Team ID
p_seq            order in which the pitcher appeared
                 (1 = starter, 2 = 2nd pitcher of game, etc.)
stattype         'value', 'official', 'lower', or 'upper': 
                 'upper' and 'lower' bounds are shown for some stats to highlight data uncertainty
                 'official' identifies discrepancies between Retrosheet accounts and official statistics
p_ipouts         outs recorded (IP * 3)
p_noout          batters faced by in any innings in which no outs were recorded
p_bfp            batters faced by pitcher
p_h              hits
p_d              doubles
p_t              triples
p_hr             home runs
p_r              runs
p_er             earned runs
p_w              walks allowed
p_iw             intentional walks
p_k              strikeouts
p_hbp            hit by pitches
p_wp             wild pitches
p_bk             balks allowed
p_sh             sacrifice hits
p_sf             sacrifice flies
p_sb             stolen bases allowed
p_cs             caught stealing allowed
p_pb             passed balls allowed
wp               winning pitcher?
lp               losing pitcher?
save             did player earn a save?
gs               game started
gf               game finished (in relief)
cg               complete game
date             date of game
number           game number (0 if single game; 1, 2, etc. if multiple games on same date)
site             location of game
vishome		 'v' if team was visiting team; 'h' if team was home team
opp              team's opponent in game
win              equal to one if team won the game
loss		 equal to one if team lost the game
tie		 equal to one if game ended in a tie
gametype         type of game (e.g., regular-season, exhibition, etc.)
box              do we have a box score; blank indicates 'no'
pbp              do we have a play-by-play account; d = deduced account, y = play-by-play account from newspaper or scorecard; blank indicates 'no'

fielding.csv
------------
gid              Game ID
id               Player ID
team             Team ID
d_seq            order of positions (if player played multiple positions - 1 = first position, etc.)
d_pos            fielding position
stattype         'value', 'official', 'lower', or 'upper': 
                 'upper' and 'lower' bounds are shown for some stats to highlight data uncertainty
                 'official' identifies discrepancies between Retrosheet accounts and official statistics
d_ifouts         defensive outs for which player played this position
d_po             putouts
d_a              assists
d_e              errors
d_dp             double plays
d_tp             triple plays
d_pb             passed balls allowed
d_wp             wild pitches allowed
d_sb             stolen bases allowed
d_cs             caught stealing - the stats 'd_pb', d_wp', 'd_sb', and 'd_cs' are compiled only for catchers (d_pos = 2)
d_gs             games started at fielding position
date             date of game
number           game number (0 if single game; 1, 2, etc. if multiple games on same date)
site             location of game
vishome		 'v' if team was visiting team; 'h' if team was home team
opp              team's opponent in game
win              equal to one if team won the game
loss		 equal to one if team lost the game
tie		 equal to one if game ended in a tie
gametype         type of game (e.g., regular-season, exhibition, etc.)
box              do we have a box score; blank indicates 'no'
pbp              do we have a play-by-play account; d = deduced account, y = play-by-play account from newspaper or scorecard; blank indicates 'no'

plays.csv
---------
gid              Game ID
event            play as it appears in our event file
inning           inning
top_bot          top (0) or bottom (1) of inning
vis_home         visiting (0) or home (1) team batting
site	         location (ballpark) of event
                 - if a game was suspended and later resumed at a different site, the value for 'site' will change mid-game,
                   indicating the site of specific events
batteam          batting team
pitteam          pitching team
score_v          visiting team score at start of play
score_h          home team score at start of play
batter           batter
pitcher          pitcher
lp               lineup position of batter
bat_f            fielding position of batter
bathand          batter handedness (B, L, R)
pithand          pitcher handedness
balls            number of balls
strikes          number of strikes
count            pitch count, if known; '??' if unknown
                 - balls, strikes, and count do not include final pitch of play
pitches          pitch sequence, if known; blank if unknown
nump		 number of pitches, if known; blank if unknown
pa               did the play result in a plate appearance? (1 if yes, 0 if no)
ab               at bat
single           single
double           double
triple           triple
hr               home run
sh               sacrifice bunt
sf               sacrifice fly, if awarded; in some seasons, sh and sf were combined in official stats; they are separated here
hbp              hit-by-pitch
walk             walk
k                strikeout
xi               batter reached on interference or obstruction, no at bat
roe              batter reached on an error
fc               batter reached on a fielder's choice
othout           batting out not specified otherwise
noout            plate appearance not otherwise specified
                 - plate appearances will be equal to exactly one of the 14 preceding columns ('single' - 'noout')
oth              sum of 'othout' and 'noout' (legacy from earlier releases of 'plays')
bip              ball-in-play; the hit type for bip is identified in the next four columns, if known
bunt             bunt
ground           ground ball
fly              fly ball (or pop up)
line             line drive
iw               intentional walk - subset of 'walk'
gdp              grounded into double play; 'gdp' is an official stat
othdp            double play that was not a 'gdp'
tp               triple play
fle              dropped foul-ball error (batter remains at bat)
wp               wild pitch
pb               passed ball
bk               balk
oa               non-pa not identified by any other stats: either 'out advancing' or 'other advance'
di               defensive indifference
sb2              stolen base of second base
sb3              stolen base of third base
sbh              stolen base of home
cs2              caught stealing second base
cs3              caught stealing third base
csh              caught stealing home
pko1             pickoff at first base
pko2             pickoff at second base
pko3             pickoff at third base
k_safe           strikeout on which the batter reached base safely - subset of 'k'
e1               error(s) by the pitcher; it is possible to be charged multiple errors on a single play
e2               error(s) by the catcher
e3               error(s) by the first baseman
e4               error(s) by the second baseman
e5               error(s) by the third baseman
e6               error(s) by the shortstop
e7               error(s) by the left fielder
e8               error(s) by the center fielder
e9               error(s) by the right fielder
outs_pre         number of outs prior to the play
outs_post        number of outs at the conclusion of the play
br1_pre          runner on first base, if any, prior to the play
br2_pre          runner on second base, if any, prior to the play
br3_pre          runner on third base, if any, prior to the play
br1_post         runner on first base, if any, at the conclusion of the play
br2_post         runner on second base, if any, at the conclusion of the play
br3_post         runner on third base, if any, at the conclusion of the play
lob_id1          id of baserunner left on base after third out of inning
lob_id2          id of baserunner left on base after third out of inning
lob_id3          id of baserunner left on base after third out of inning
pr1_pre          pitcher responsible for runner on first base prior to the play
pr2_pre          pitcher responsible for runner on second base prior to the play
pr3_pre          pitcher responsible for runner on third base prior to the play
pr1_post         pitcher responsible for runner on first base at the conclusion of the play
pr2_post         pitcher responsible for runner on second base at the conclusion of the play
pr3_post         pitcher responsible for runner on third base at the conclusion of the play
run_b            batter if he scored a run on the play
run1             runner on first base if he scored a run on the play
run2             runner on second base if he scored a run on the play
run3             runner on third base if he scored a run on the play
prun_b		 pitcher charged with the run scored by the batter
prun1            pitcher charged with the run scored by the runner on first base
prun2            pitcher charged with the run scored by the runner on second base
prun3            pitcher charged with the run scored by the runner on third base
ur_b		 unearned run scored by batter
ur1		 unearned run scored by Unearned runner on first base
ur2		 unearned run scored by Unearned runner on second base
ur3		 unearned run scored by Unearned runner on third base
rbi_b		 RBI credited for run scored by batter
rbi1		 RBI credited for run scored by runner on first base
rbi2		 RBI credited for run scored by runner on second base
rbi3		 RBI credited for run scored by runner on third base
runs             total runs scored on the play
rbi              total RBI credited to batter on the play
er               total earned runs scored on the play
tur              runs scored on the play credited as (TUR) - team unearned runs
                 - in modern seasons, relief pitchers do not get the benefit of errors which occurred
                   before they entered the game in calculating pitcher-specific earned runs; team
                   earned runs are calculated taking into account all errors in the inning
l1	 	 batting team lineup position 1
l2		 batting team lineup position 2
l3		 batting team lineup position 3
l4		 batting team lineup position 4
l5		 batting team lineup position 5
l6		 batting team lineup position 6
l7		 batting team lineup position 7
l8		 batting team lineup position 8
l9		 batting team lineup position 9
lf1		 fielding position of batting team lineup position 1
lf2		 fielding position of batting team lineup position 2
lf3		 fielding position of batting team lineup position 3
lf4		 fielding position of batting team lineup position 4
lf5		 fielding position of batting team lineup position 5
lf6		 fielding position of batting team lineup position 6
lf7		 fielding position of batting team lineup position 7
lf8		 fielding position of batting team lineup position 8
lf9	 	 fielding position of batting team lineup position 9
f2               catcher
f3               first baseman
f4               second baseman
f5               third baseman
f6               shortstop
f7               left fielder
f8               center fielder
f9               right fielder
po0              putouts on play for which fielder is not identified (scored as 99 in event files)
po1              putouts on play by pitcher
po2              putouts on play by catcher
po3              putouts on play by first baseman
po4              putouts on play by second baseman
po5              putouts on play by third baseman
po6              putouts on play by shortstop
po7              putouts on play by left fielder
po8              putouts on play by center fielder
po9              putouts on play by right fielder
a1               assists on play by pitcher
a2               assists on play by catcher
a3               assists on play by first baseman
a4               assists on play by second baseman
a5               assists on play by third baseman
a6               assists on play by shortstop
a7               assists on play by left fielder
a8               assists on play by center fielder
a9               assists on play by right fielder
fseq		 fielding sequence for out(s): e.g., 643 for 6-4-3 double play (0 indicates unknown fielder)
batout1          fielding position of player who initiated first batting out
batout2          fielding position of player who initiated second batting out (for double play)
batout3          fielding position of player who initiated third batting out (for triple play)
brout_b          fielding position of player who initiated baserunning out by batter
brout1           fielding position of player who initiated baserunning out by runner on first base
brout2           fielding position of player who initiated baserunning out by runner on second base
brout3           fielding position of player who initiated baserunning out by runner on third base
                 - for assisted plays, the player who 'initiated' an out is the player who earned the first assist
firstf           fielding position of first player to field a ball-in-play (1 - 9; 0 if unknown)
loc              location of ball in play, if known
hittype          hit type of ball in play, if known
                 - BG = bunt ground ball, BP = bunt pop-up, BL = bunt line drive, G = ground ball, P = pop up, F = fly ball, L =line drive
dpopp            equal to one if there was a runner on first base and fewer than two outs
pivot            pivot man on double-play opportunity if one existed
                 - on a ground ball where 'dpopp' = 1, 'pivot' is player who made initial putout if a baserunner was forced out,
                   or fielder of ground ball ('firstf') if no force out
                 - idea behind 'pivot' is to establish fielders who deserve to share credit/blame for converting double plays
pn               play number - sequential order of plays within a particular game
umphome		 home plate umpire
ump1b		 1b umpire
ump2b		 2b umpire
ump3b		 3b umpire
umplf		 lf umpire
umprf		 rf umpire
date		 date of game
                 - if a game was suspended and later resumed on a different date, the value for 'date' will change mid-game,
                   to indicate the resumption of the game; 'date' does not change mid-game for games that are played continuously,
   		   but end after midnight local time
gametype         type of game (e.g., regular-season, exhibition, etc.)
pbp              'deduced' or 'full'



link for CSV: https://www.retrosheet.org/downloads/csvdownloads.zip
