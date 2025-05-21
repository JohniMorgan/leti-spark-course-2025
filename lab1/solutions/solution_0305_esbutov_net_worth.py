from pyspark.shell import spark
from pyspark.shell import spark
from pyspark.sql import DataFrame

from common import read_csv, view

def solve() -> DataFrame:
    match = read_csv('match')
    player = read_csv('player')
    player_result = read_csv('player_result')

    view("match", match)
    view("player", player)
    view("player_result", player_result)

    # Создаём представление с золотом игроков в матчах
    view("player_gold", """
        select
            pr.player_id,
            pr.match_id,
            pr.gold
        from
            player_result pr
    """)

    # Генерируем уникальные пары игроков в каждом матче
    view("player_pairs", """
        select
            a.match_id,
            least(a.player_id, b.player_id) as player1_id,
            greatest(a.player_id, b.player_id) as player2_id,
            (a.gold + b.gold) as total_gold
        from
            player_gold a
        inner join
            player_gold b
        on
            a.match_id = b.match_id
            and a.player_id != b.player_id
        where
            a.player_id < b.player_id
    """)

    # Вычисляем средний суммарный Net Worth для каждой пары
    view("avg_net_worth", """
        select
            player1_id,
            player2_id,
            avg(total_gold) as avg_total_net_worth,
            count(*) as matches_played_together
        from
            player_pairs
        group by
            player1_id, player2_id
    """)

    # Добавляем имена игроков
    view("result", """
        select
            p1.name AS player1_name,
            p2.name AS player2_name,
            anw.avg_total_net_worth,
            anw.matches_played_together
        from
            avg_net_worth anw
        join
            player p1 on anw.player1_id = p1.player_id
        join
            player p2 on anw.player2_id = p2.player_id
        order by
            anw.avg_total_net_worth desc
    """)

    # Разблокировать для вывода результата
    # return spark.sql("select * from result")

    # Разблокировать чтобы посмотреть 
    return spark.sql("select * from player_pairs where player1_id = 1 and player2_id = 54")
