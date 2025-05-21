from pyspark.shell import spark
from pyspark.shell import spark
from pyspark.sql import DataFrame

from common import read_csv, view


#
# Требования:
#
# N - номер (сортируем по количеству игр)
#
# name - имя
#
# pos - в каждой игре участвует 10 игроков (5 в каждой команде)
# в каждой команде игроки занимают "позицию" от 1 до 5.
# говорят, например, в этой игре он играет на ЧЕТВЕРТОЙ позиции
# вот в этой колонке требуется посчитать на какой позиции человек играл больше всего
#
# если игр однинаково на нескольких позициях
# (10 игр на первой позиции и 10 на второй то выбираем НАИМЕНЬШУЮ - в данном случае первую)
#
# kda - среднее число kills/deaths/assists
#
# avg_gold - среднее количество gold
#
# winrate - процент побед игрока на протяжении всех его игр
#

def solve() -> DataFrame:
    match = read_csv('match')
    player = read_csv('player')
    player_result = read_csv('player_result')

    view("match", match)
    view("player", player)
    view("player_result", player_result)

    # Сформируем таблицу соответствий игрок-матч-поражение, где true будет означать поражение
    # Дабы сохранить хронологию, и не перепутать в дальнейшем как мы считаем лузстрик вводим и дату окончания матча
    view("player_match_loss", """
        select 
            player_result.player_id, 
            match.match_id,
            case when player_result.is_radiant != match.radiant_won then 1 else 0 end as is_loss,
            match.finished_at
        from
            player_result
        left join 
            match on match.match_id = player_result.match_id 
        order by match.finished_at
    """)

    # Здесь мы посчитаем оконной функцией суммирования поражения. Логика такая:
    # Если у нас поражение - прибавляем 0, т.е. оставляем значение
    # Если победа - прибавим единицу, выводя новую "группу" лузстрика.
    # Уже на этом этапе мы получим кол-во лузстриков для каждого игрока (максимальное число группы)
    # partition гарантирует, что группы лузстриков между игроками не пересекутся
    view("player_loss_streak", """
        select
            player_id,
            match_id,
            is_loss,
            sum(case when is_loss = 0 then 1 else 0 end)
                over(partition by player_id order by finished_at) as loss_streak
        from
            player_match_loss
    """)

    # Дальше мы сгруппируем по числовым группа лузстрика,
    # подсчитав кол-во элементов в каждой группе. Группировка будет
    # двойной, т.к. ещё необходимо подсчитать по каждому игроку
    view("player_loss_length", """
        select
            player_id,
            loss_streak,
            count(*) as streak_length
        from
            player_loss_streak
        WHERE
            is_loss = 1
        GROUP BY
            player_id, loss_streak
    """)

    # Вычисляем среднюю длину лоустриков
    view("player_avg_losestreak", """
        select
            player_id,
            coalesce(avg(streak_length), 0) as avg_losestreak
        from
            player_loss_length
        GROUP BY
            player_id
    """)

    # Добавляем имя игрока
    view("result", """
        select
            p.player_id,
            p.name,
            ls.avg_losestreak
        from
            player_avg_losestreak ls
        join
            player p on ls.player_id = p.player_id
    """)

    
    # return spark.sql("select * from result order by avg_losestreak desc")
    # Разблокировать для ручного теста
    return spark.sql("select * from player_match_loss where player_id = 4")
