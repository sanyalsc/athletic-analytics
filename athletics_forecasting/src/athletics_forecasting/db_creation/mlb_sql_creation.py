import sqlite3

def create_batting_db():
    
    with sqlite3.connect('team_data.db') as conn:
        c = conn.cursor()

        c.execute(
            """CREATE TABLE batters (
            player text,
            position text,
            age real,
            in2 int,
            in3 int,
            in4 int,
            in5 int,
            in6 int,
            in7 int,
            in8 int,
            in9 int,
            run int,
            hit int,
            error int,
            streak int,
            div_rank int,
            lg_rank int,
            mlb_rank int,
            wins int,
            losses int,
            date text
            )"""
        )
        conn.commit()


def create_team_db():
    
    with sqlite3.connect('team_data.db') as conn:
        c = conn.cursor()

        c.execute(
            """CREATE TABLE teams (
            team text,
            run int,
            hit int,
            error int,
            div_rank int,
            lg_rank int,
            mlb_rank int,
            win bool,
            opponent text,
            date text,
            wins int,
            games_played int,
            year text,
            streak int
            )"""
        )
        conn.commit()


def create_pitching_db():
    
    with sqlite3.connect('team_data.db') as conn:
        c = conn.cursor()

        c.execute(
            """CREATE TABLE pitchers (
            team text,
            in1 int,
            in2 int,
            in3 int,
            in4 int,
            in5 int,
            in6 int,
            in7 int,
            in8 int,
            in9 int,
            run int,
            hit int,
            error int,
            streak int,
            div_rank int,
            lg_rank int,
            mlb_rank int,
            wins int,
            losses int,
            date text
            )"""
        )
        conn.commit()


def create_odds_table():
    with sqlite3.connect('team_data.db') as conn:
        c = conn.cursor()

        c.execute(
            """CREATE TABLE odds (
            team text,
            date text,
            open INT,
            close INT,
            run_line INT,
            run_odds INT,
            ou_open INT,
            ou_open_odds INT,
            ou_close INT,
            ou_close_odds INT
            )"""
        )
        conn.commit()

if __name__ == '__main__':
    create_batting_db()
    create_pitching_db()
    create_team_db()
    create_odds_table()