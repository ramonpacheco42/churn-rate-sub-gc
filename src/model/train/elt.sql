CREATE TABLE IF NOT EXISTS Analytics.abt_model_churn AS
WITH tb_features AS (
SELECT
    t1.*,
    t3.qtMedalhaDist,
    t3.qtMedalha,
    t3.qtMedalhaTribo,
    t3.qtExpBatalha,
    t2.qtPartidas,
    t2.qtDias,
    t2.propDia01,
    t2.propDia02,
    t2.propDia03,
    t2.propDia04,
    t2.propDia05,
    t2.probDia06,
    t2.propDia07,
    t2.qtRecencia,
    t2.winRate,
    t2.avgHsRate,
    t2.vlHsHate,
    t2.avgKDA,
    t2.vlKDA,
    t2.vlKDR,
    t2.avgKill,
    t2.avgAssist,
    t2.avgDeath,
    t2.avgHs,
    t2.avgBombeDefuse,
    t2.avgBombePlant,
    t2.avgTk,
    t2.avgTkAssist,
    t2.avg1Kill,
    t2.avg2Kill,
    t2.avg3Kill,
    t2.avg4Kill,
    t2.avg5Kill,
    t2.avgPlusKill,
    t2.avgFirstKill,
    t2.avgDamage,
    t2.avgHits,
    t2.avgShots,
    t2.avgLastAlive,
    t2.avgClutchWon,
    t2.avgRoundsPlayed,
    t2.avgSurvived,
    t2.avgTrade,
    t2.avgFlashAssist,
    t2.propAncient,
    t2.propOverpass,
    t2.propVertigo,
    t2.propNuke,
    t2.propTrain,
    t2.propMirage,
    t2.propDust2,
    t2.propInferno,
    t2.vlLevel
FROM Analytics.tb_assinaturas_gc AS t1
LEFT JOIN Analytics.tb_gameplay_gc AS t2
ON t1.dtRef = t2.dtRef
AND t1.idPlayer = t2.idPlayer
LEFT JOIN Analytics.tb_medalha_gc AS t3
ON t1.dtRef = t3.dtRef
AND t1.idPlayer = t3.idPlayer
WHERE t1.dtRef <= DATE_SUB('2022-02-10', INTERVAL 30 DAY)
)
SELECT
	t1.*,
	COALESCE(t2.flAssinatura,0) AS flNaoChurn
FROM tb_features AS t1
LEFT JOIN Analytics.tb_assinaturas_gc AS t2
ON t1.idPlayer = t2.idPlayer 
AND t1.dtRef = DATE_SUB(t2.dtRef, INTERVAL 30 DAY)


