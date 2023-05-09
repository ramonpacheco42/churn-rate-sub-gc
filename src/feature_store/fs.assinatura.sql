WITH tb_assinaturas AS (
    SELECT t1.*,
            t2.descMedal
    FROM gc.tb_players_medalha AS t1
    LEFT JOIN gc.tb_medalha AS t2
    ON t1.idMedal = t2.idMedal
    WHERE t1.dtCreatedAt < t1.dtExpiration
    AND t1.dtCreatedAt < COALESCE(t1.dtRemove, t1.dtExpiration ,NOW())
    AND t1.dtCreatedAt < '{date}'
    AND COALESCE(t1.dtRemove, t1.dtExpiration ,NOW()) > '{date}'
    AND t2.descMedal IN ('Membro Premium', 'Membro Plus')
    ORDER BY idPlayer
),
tb_assinaturas_rn AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY idPlayer ORDER BY dtExpiration DESC) AS rn_assinatura
    FROM tb_assinaturas
),
tb_assinatura_sumario AS (
    SELECT
        *,
        DATEDIFF('{date}', dtCreatedAt) AS qtDiasAssinatura,
        DATEDIFF(dtExpiration, '{date}') AS qtDiasExpiracaoAssinatura
    FROM tb_assinaturas_rn
    WHERE rn_assinatura = 1
    ORDER BY idPlayer
),
tb_assinatura_hist AS (
    SELECT 
    t1.idPlayer,
    COUNT(t1.idMedal) AS qtAssinatura,
    COUNT(CASE WHEN t2.descMedal = 'Membro Premium' THEN t1.idMedal END) AS qtPremium,
    COUNT(CASE WHEN t2.descMedal = 'Membro Plus' THEN t1.idMedal END) AS qtPlus
FROM gc.tb_players_medalha AS t1
LEFT JOIN gc.tb_medalha AS t2
ON t1.idMedal = t2.idMedal
    WHERE t1.dtCreatedAt < t1.dtExpiration
    AND t1.dtCreatedAt < COALESCE(t1.dtRemove, NOW())
    AND t1.dtCreatedAt < '{date}'
    AND t2.descMedal IN ('Membro Premium','Membro Plus')
GROUP BY t1.idPlayer
)
SELECT
    '{date}' AS dtRef,
    t1.idPlayer,
    t1.descMedal,
    1 AS flAssinatura,
    t1.qtDiasAssinatura,
    t1.qtDiasExpiracaoAssinatura,
    t2.qtAssinatura,
    t2.qtPremium,
    t2.qtPlus
FROM tb_assinatura_sumario AS t1
LEFT JOIN tb_assinatura_hist AS t2
ON t1.idPlayer = t2.idPlayer