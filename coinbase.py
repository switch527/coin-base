import logging
import sys
import time
from queue import Queue
from threading import Thread

from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from btfx_trader import CoinAPI

Base = declarative_base()

log = logging.getLogger('coinbase')


class Tickers(Base):
    __tablename__ = 'tickers'
    _id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    symbol = Column(String(8), nullable=False)
    bid = Column(Float, nullable=False)
    bid_size = Column(Float, nullable=False)
    ask = Column(Float, nullable=False)
    ask_size = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    change_perc = Column(Float, nullable=False)
    last_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)


class Books(Base):
    __tablename__ = 'books'
    _id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    symbol = Column(String(8), nullable=False)
    price = Column(Float, nullable=False)
    sig_fig = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)


class RawBooks(Base):
    __tablename__ = 'raw_books'
    _id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    symbol = Column(String(8), nullable=False)
    id = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)


class Trades(Base):
    __tablename__ = 'trades'
    _id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    symbol = Column(String(8), nullable=False)
    id = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    price = Column(Float, nullable=False)


class Candles(Base):
    __tablename__ = 'candles'
    _id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    symbol = Column(String(8), nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)


class CoinBase:
    models = {
        'tickers': Tickers,
        'books': Books,
        'raw_books': RawBooks,
        'trades': Trades,
        'candles': Candles

    }

    def __init__(self, path, symbols, log_level='INFO'):
        # setup logging
        for _log in [logging.getLogger('btfx_trader'), log]:
            _log.setLevel(log_level)
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(log_level)
            formatter = logging.Formatter(fmt='%(levelname)-5s: %(name)-15s: %(message)s')
            stream_handler.setFormatter(formatter)
            _log.addHandler(stream_handler)

        if isinstance(symbols, str):
            pass  # load from disk
        db = create_engine(path)
        Base.metadata.create_all(db)
        Base.metadata.bind = db
        self.maker = sessionmaker(bind=db)
        self.commit_every = 10
        self.api = CoinAPI(symbols)
        self.running = True
        self.queue = Queue()

    def stream(self, symbol, _type):
        while self.running:
            datum = self.api.get(symbol, _type)
            self.queue.put((_type, datum))

    def save(self):
        while self.running:
            session = self.maker()
            count = 0
            while not self.queue.empty():
                _type, datum = self.queue.get()
                session.add(self.models[_type](**datum))
                log.debug('Added %s for %s to database', _type, datum.get('symbol', 'None'))
                if count % self.commit_every == 0:
                    session.commit()
                    log.debug('Successfully committed addition')
                self.queue.task_done()
                count += 1
            session.commit()
            session.close()
            time.sleep(1)

    def run(self):
        try:
            self.api.connect()
        except KeyboardInterrupt:
            return
        log.debug('Starting data cleaning streams')
        workers = []
        for symbol in self.api.symbols:
            for _type in self.api.data_types:
                w = Thread(target=self.stream, args=(symbol, _type))
                w.start()
                workers.append(w)
        try:
            self.save()
        except KeyboardInterrupt:
            log.debug('Cancelled, waiting for all data streams to close')
            self.running = False
            log.info('Successfully closed database')
        self.api.shutdown()
