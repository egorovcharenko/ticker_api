"""

This Molotov script has 2 scenario

"""
from molotov import scenario


_API = 'http://localhost:8888/ticker/btc_usd'


@scenario(weight=100)
async def scenario_one(session):
    async with session.get(_API) as resp:
        res = await resp.json()
        assert resp.status == 200

