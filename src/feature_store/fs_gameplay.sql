WITH tb_level AS (
   SELECT
        idPlayer,
        vlLevel,
        ROW_NUMBER() OVER (PARTITION BY idPlayer ORDER BY dtCreatedAt DESC) AS rnPlayer
    FROM gc.tb_lobby_stats_player
    WHERE dtCreatedAt < '{date}'
    AND dtCreatedAt >= DATE_SUB('{date}', INTERVAL 30 DAY)
    ORDER BY idPlayer, dtCreatedAt
),
tb_level_final AS(
    SELECT * FROM tb_level
    WHERE rnPlayer = 1  
),
tb_gameplay_stats AS (
        SELECT
        idPlayer,
        COUNT(DISTINCT  idLobbyGame) AS qtPartidas,
        COUNT(DISTINCT DATE(dtCreatedAt)) AS qtDias,
        COUNT(DISTINCT CASE WHEN DAYOFWEEK(dtCreatedAt) = 1 THEN DATE(dtCreatedAt) END) / COUNT(DISTINCT DATE(dtCreatedAt)) AS propDia01,
        COUNT(DISTINCT CASE WHEN DAYOFWEEK(dtCreatedAt) = 2 THEN DATE(dtCreatedAt) END) / COUNT(DISTINCT DATE(dtCreatedAt)) AS propDia02,
        COUNT(DISTINCT CASE WHEN DAYOFWEEK(dtCreatedAt) = 3 THEN DATE(dtCreatedAt) END) / COUNT(DISTINCT DATE(dtCreatedAt)) AS propDia03,
        COUNT(DISTINCT CASE WHEN DAYOFWEEK(dtCreatedAt) = 4 THEN DATE(dtCreatedAt) END) / COUNT(DISTINCT DATE(dtCreatedAt)) AS propDia04,
        COUNT(DISTINCT CASE WHEN DAYOFWEEK(dtCreatedAt) = 5 THEN DATE(dtCreatedAt) END) / COUNT(DISTINCT DATE(dtCreatedAt)) AS propDia05,
        COUNT(DISTINCT CASE WHEN DAYOFWEEK(dtCreatedAt) = 6 THEN DATE(dtCreatedAt) END) / COUNT(DISTINCT DATE(dtCreatedAt)) AS probDia06,
        COUNT(DISTINCT CASE WHEN DAYOFWEEK(dtCreatedAt) = 7 THEN DATE(dtCreatedAt) END) / COUNT(DISTINCT DATE(dtCreatedAt)) AS propDia07,
        MIN(DATEDIFF('{date}',dtCreatedAt)) AS qtRecencia,
        AVG(flWinner) AS winRate,
        AVG(qtHs / qtKill) AS avgHsRate,
        SUM(qtHs) / SUM(qtKill) AS vlHsHate,
        AVG((qtKill + qtAssist) / COALESCE(qtDeath,1)) AS avgKDA,
        COALESCE(SUM(qtKill + qtAssist)/SUM(COALESCE(qtDeath,1)),0) AS vlKDA,
        SUM(COALESCE(qtKill,0)) / SUM(COALESCE(qtDeath,1)) AS vlKDR,
        avg(qtKill) as avgKill,
        avg(qtAssist) as avgAssist,
        avg(qtDeath) as avgDeath,
        avg(qtHs) as avgHs,
        avg(qtBombeDefuse) as avgBombeDefuse,
        avg(qtBombePlant) as avgBombePlant,
        avg(qtTk) as avgTk,
        avg(qtTkAssist) as avgTkAssist,
        avg(qt1Kill) as avg1Kill,
        avg(qt2Kill) as avg2Kill,
        avg(qt3Kill) as avg3Kill,
        avg(qt4Kill) as avg4Kill,
        avg(qt5Kill) as avg5Kill,
        avg(qtPlusKill) as avgPlusKill,
        avg(qtFirstKill) as avgFirstKill,
        avg(vlDamage) as avgDamage,
        avg(qtHits) as avgHits,
        avg(qtShots) as avgShots,
        avg(qtLastAlive) as avgLastAlive,
        avg(qtClutchWon) as avgClutchWon,
        avg(qtRoundsPlayed) as avgRoundsPlayed,
        avg(qtSurvived) as avgSurvived,
        avg(qtTrade) as avgTrade,
        avg(qtFlashAssist) as avgFlashAssist,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_ancient' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propAncient,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_overpass' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propOverpass,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_vertigo' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propVertigo,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_nuke' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propNuke,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_train' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propTrain,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_mirage' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propMirage,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_dust2' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propDust2,
        COUNT(DISTINCT CASE WHEN descMapName = 'de_inferno' THEN idLobbyGame END) / COUNT(DISTINCT idLobbyGame) AS propInferno
    FROM gc.tb_lobby_stats_player
    WHERE dtCreatedAt < '{date}'
    AND dtCreatedAt >= DATE_SUB('{date}', INTERVAL 30 DAY)
    GROUP BY idPlayer
    ORDER BY COUNT(DISTINCT  idLobbyGame) DESC
)

SELECT 
        '{date}' AS dtRef,
        T1.*,
        T2.vlLevel
FROM tb_gameplay_stats AS T1
LEFT JOIN tb_level_final AS T2
ON T1.idPlayer = T2.idPlayer