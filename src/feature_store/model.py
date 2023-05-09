import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()
Base = declarative_base()

host = os.environ['DB_HOST']
password = os.environ['DB_PASSWORD']
user = os.environ['DB_USER']
port = os.environ['DB_PORT']
database = os.environ['DB_DATABASE_ANALYTICS']

class Analytics_table(Base):
    __tablename__ = 'tb_assinaturas_gc'
    pkUser = Column(Integer, primary_key=True)
    dtRef = Column(Date)
    idPlayer = Column(Integer)
    descMedal = Column(String(50))
    flAssinatura = Column(Integer)
    qtDiasAssinatura = Column(Integer)
    qtDiasExpiracaoAssinatura = Column(Integer)
    qtAssinatura = Column(Integer)
    qtPremium = Column(Integer)
    qtPlus = Column(Integer)

    def start(self):
        db_string = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'
        engine = create_engine(db_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        Base.metadata.create_all(engine)
        return session, engine

    def get_assinaturas(df,engine):
        df.to_sql('tb_assinaturas_gc',engine, index=False,if_exists='append')
        return print(f'\n Foram inseridas {df.shape[0]} no banco de dados!')
    
class create_tb_medalha(Base):
    __tablename__ = 'tb_medalha_gc'
    pkUser = Column(Integer, primary_key=True)
    dtRef = Column(Date)
    idPlayer = Column(Integer)
    qtMedalhaDist = Column(Integer)
    qtMedalha = Column(Integer)
    qtMedalhaTribo = Column(Integer)
    qtExpBatalha = Column(Integer)

    def __init__(self):
        session, engine = Analytics_table().start()
        self.session = session
        self.engine = engine

    def get_medalhas(df,engine):
        df.to_sql('tb_medalha_gc',engine, index=False,if_exists='append')
        return print(f'\n Foram inseridas {df.shape[0]} no banco de dados!')


class create_tb_gameplay(Base):
    __tablename__ = 'tb_gameplay_gc'
    pkUser = Column(Integer, primary_key=True)
    dtRef = Column(Date)
    idPlayer 	= Column(Integer)
    qtPartidas	= Column(Integer)
    qtDias	= Column(Integer)
    propDia01	= Column(Float)
    propDia02	= Column(Float)
    propDia03	= Column(Float)
    propDia04	= Column(Float)
    propDia05	= Column(Float)
    probDia06	= Column(Float)
    propDia07	= Column(Float)
    qtRecencia	= Column(Integer)
    winRate	= Column(Float)
    avgHsRate	= Column(Float)
    vlHsHate	= Column(Float)
    avgKDA	= Column(Float)
    vlKDA	= Column(Float)
    vlKDR	= Column(Float)
    avgKill	= Column(Float)
    avgAssist	= Column(Float)
    avgDeath	= Column(Float)
    avgHs	= Column(Float)
    avgBombeDefuse	= Column(Float)
    avgBombePlant	= Column(Float)
    avgTk	= Column(Float)
    avgTkAssist	= Column(Float)
    avg1Kill	= Column(Float)
    avg2Kill	= Column(Float)
    avg3Kill = Column(Float)
    avg4Kill	= Column(Float)
    avg5Kill = Column(Float)
    avgPlusKill	= Column(Float)
    avgFirstKill = Column(Float)
    avgDamage	= Column(Float)
    avgHits	= Column(Float)
    avgShots	= Column(Float)
    avgLastAlive	= Column(Float)
    avgClutchWon	= Column(Float)
    avgRoundsPlayed	= Column(Float)
    avgSurvived	= Column(Float)
    avgTrade	= Column(Float)
    avgFlashAssist	= Column(Float)
    propAncient	= Column(Float)
    propOverpass	= Column(Float)
    propVertigo	= Column(Float)
    propNuke	= Column(Float)
    propTrain	= Column(Float)
    propMirage	= Column(Float)
    propDust2	= Column(Float)
    propInferno	= Column(Float)
    vlLevel = Column(Integer)

    def __init__(self):
        session, engine = Analytics_table().start()
        self.session = session
        self.engine = engine
    
    def get_gameplay(df,engine):
        df.to_sql('tb_gameplay_gc',engine, index=False,if_exists='append')
        return print(f'\n Foram inseridas {df.shape[0]} no banco de dados!')