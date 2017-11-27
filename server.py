import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from sqlalchemy import inspect
from aioserverplus import Handler, serve

from coinbase import CoinBase


class CoinHandler(Handler):
    since_convert = {'s': 1, 'm': 60, 'h': 3600, 'd': 3600 * 24, 'w': 3600 * 24 * 7}

    def __init__(self, handlers, *args, **kwargs):
        super(CoinHandler, self).__init__(handlers, *args, **kwargs)
        self.db = CoinBase("sqlite:///coinData.db",
                           ["BTCUSD"#, "LTCUSD", "ETHUSD", "EDOUSD", "BCCUSD"#,
                            #"ETCUSD", "RRTUSD", "ZECUSD", "XMRUSD", "DSHUSD",
                            #"BCUUSD", "XRPUSD", "IOTUSD", "EOSUSD", "SANUSD",
                            #"BCHUSD", "NEOUSD", "ETPUSD", "QTMUSD", "BT1USD",
                            #"AVTUSD", "BTGUSD", "DATUSD", "OMGUSD", "BT2USD"
                            ])

    async def handle_request(self, request):
        symbols = [(i.upper() + "USD") if not i.endswith("USD") else i for i in request.params['symbols'].split(',')]
        _type = request.pth.split('/')[-1]
        _since = request.params.get('since', None) or datetime.now() - timedelta(days=1)
        _to = request.params.get('to', None) or datetime.now()
        _to = datetime.fromtimestamp(float(_to)) if isinstance(_to, str) else _to
        if isinstance(_since, str):
            if any(k in _since for k in self.since_convert):
                _since = datetime.now() - timedelta(seconds=int(_since[:-1]) * self.since_convert[_since[-1]])
            else:
                _since = datetime.fromtimestamp(float(_since))

        model = self.db.models[_type]
        keys = [i for i in inspect(model).columns.keys() if i not in ['_id']]
        session = self.db.maker()
        query = session.query(model).\
            filter(model.symbol.in_(symbols)).\
            filter(model.time >= _since).\
            filter(model.time <= _to).\
            order_by(model.time)
        res = []
        for i, row in enumerate(query):
            data = {q: getattr(row, q) for q in keys}
            data.update(time=data['time'].timestamp())
            res.append(data)
        session.close()
        return json.dumps({'data': res})


async def backend(handler):
    pool = ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(pool, handler.db.run)
    await asyncio.wait_for(future, timeout=None)
    pool.shutdown()


if __name__ == '__main__':
    ADDRESS = 'localhost', 9900
    serve(address=ADDRESS, log_modules=['aioserverplus', 'coinbase'],
          handler=CoinHandler, handler_args=({},),
          backend=backend)
