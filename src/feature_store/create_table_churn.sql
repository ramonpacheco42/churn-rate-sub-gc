CREATE TABLE IF NOT EXISTS an_assinaturas_gc (
    dtRef DATE NOT NULL,
    idPlayer INT NOT NULL,
    descMedal VARCHAR(50) NOT NULL,
    flAssinatura INT NOT NULL,
    qtDiasAssinatura INT NOT NULL,
    qtDiasExpiracaoAssinatura INT NOT NULL,
    qtAssinatura INT NOT NULL,
    qtPremium INT NOT NULL,
    qtPlus INT NOT NULL
)